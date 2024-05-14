from aws_cdk import (
    Duration,
    aws_lambda,
    aws_iam,
    aws_sqs,
    aws_lambda_event_sources as source,
    aws_s3
)
from constructs import Construct


class LambdaFunction(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict,
                 sqs: aws_sqs.Queue, dynamodb_tables, s3_bucket: aws_s3.Bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env_name = params['env_name']
        region = params['region']
        account = params['account']

        # lambda layers
        dnspython_layer = aws_lambda.LayerVersion(
            self, "dnspython_layer",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/lambda_layer/dnspython"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_12],
            layer_version_name="dnspython_layer_{}".format(env_name),
            )
        
        # lambda function
        get_db_instance_type_lambda_role = aws_iam.Role(
            self,
            "get-db-instance-type-lambda-role-{}".format(env_name),
            role_name="get-db-instance-type-lambda-role-{}".format(env_name),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        get_db_instance_type_lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['rds:DescribeDBClusters'],
                resources=[f'arn:aws:rds:{region}:{account}:cluster:*'],
            )
        )

        get_db_instance_type_lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['rds:DescribeDBInstances'],
                resources=[f'arn:aws:rds:{region}:{account}:db:*'],
            )
        )

        get_db_instance_type_lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['ec2:DescribeNetworkInterfaces'],
                resources=['*'],
            )
        )

        self.get_db_instance_type = aws_lambda.Function(
            self, "get_db_instance_type",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/get_db_instance_type"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            role=get_db_instance_type_lambda_role,
            function_name='db-check-get-db-instance-type-{}'.format(env_name),
            layers=[dnspython_layer],
            )
        
        insert_query_to_dynamodb_lambda_role = aws_iam.Role(
            self,
            "insert-query-to-dynamodb-lambda-role-{}".format(env_name),
            role_name="insert-query-to-dynamodb-lambda-role-{}".format(env_name),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        self.insert_query_to_dynamodb = aws_lambda.Function(
            self, "insert_query_to_dynamodb",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/insert_query_to_dynamodb"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            memory_size=1024,
            function_name='db-check-insert-query-to-dynamodb-{}'.format(env_name),
            role=insert_query_to_dynamodb_lambda_role,
            environment={'REGION': region,
                         'DDB_TASK_TABLE': params['check_task_table_name'],
                         'DDB_LOG_TABLE': params['check_log_table_name'],
                         }
            )
        
        sqs.grant_send_messages(self.insert_query_to_dynamodb)
        sqs.grant_consume_messages(self.insert_query_to_dynamodb)
        sqs_source = source.SqsEventSource(sqs,
            batch_size=2000,
            max_batching_window=Duration.seconds(1),
            report_batch_item_failures=True,
            enabled=True,
        )
        self.insert_query_to_dynamodb.add_event_source(sqs_source)
        dynamodb_tables.task_table.grant_read_write_data(self.insert_query_to_dynamodb)
        dynamodb_tables.log_table.grant_read_write_data(self.insert_query_to_dynamodb)

        # Create get task progress lambda function and role
        get_task_progress_lambda_role = aws_iam.Role(
            self,
            "db-check-get-task-progress-lambda-role-{}".format(env_name),
            role_name="db-check-get-task-progress-lambda-role-{}".format(env_name),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        self.get_task_progress = aws_lambda.Function(
            self, "get_task_progress",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/get_task_progress"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            function_name='db-check-get-task-progress-{}'.format(env_name),
            role=get_task_progress_lambda_role,
            environment={'REGION': region,
                         'DDB_TASK_TABLE': params['check_task_table_name']},
        )
        dynamodb_tables.task_table.grant_read_write_data(self.get_task_progress)
        s3_bucket.grant_read_write(get_task_progress_lambda_role)

