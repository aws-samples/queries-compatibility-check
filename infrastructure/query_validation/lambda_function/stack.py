from aws_cdk import (
    Duration,
    aws_lambda,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_lambda_event_sources as sources,
    aws_iam as iam,
    aws_s3,
    aws_dynamodb
)
from constructs import Construct


class LambdaFunction(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict, vpc: ec2.Vpc, private_subnets,
                 sg: ec2.SecurityGroup, aurora_proxy: rds.DatabaseProxy,
                 s3_bucket: aws_s3.Bucket, log_table: aws_dynamodb.Table, task_table: aws_dynamodb.Table,
                 ddb_task_table_source: sources.DynamoEventSource, 
                 ddb_log_table_source: sources.DynamoEventSource, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = params['region']
        account = params['account']
        check_log_table_name = params['check_log_table_name']
        check_task_table_name = params['check_task_table_name']

        # lambda layers
        validate_python_layer = aws_lambda.LayerVersion(
            self, "validate_python_layer",
            code=aws_lambda.Code.from_asset("infrastructure/query_validation/lambda_function/lambda_layer"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_12],
            layer_version_name="validation_python_layer_{}".format(params['env_name']),
            )
        
        # lambda function
        self.validate_query_function = aws_lambda.Function(
            self, "validate_query_function",
            code=aws_lambda.Code.from_asset("infrastructure/query_validation/lambda_function/validate_query"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            function_name='db-check-validate-query-{}'.format(params['env_name']),
            layers=[validate_python_layer],
            allow_public_subnet=False,
            vpc=vpc,
            memory_size=1024,
            security_groups=[sg],
            vpc_subnets=ec2.SubnetSelection(
                subnets=private_subnets
            ),
            initial_policy=[iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['dynamodb:GetItem', 'dynamodb:UpdateItem'],
                    resources=[f"arn:aws:dynamodb:{region}:{account}:table/{check_log_table_name}",
                               f"arn:aws:dynamodb:{region}:{account}:table/{check_task_table_name}"],
                )
            ],
            environment={'PROXY_ENDPOINT': aurora_proxy.endpoint,
                         'REGION': params['region'],
                         'DDB_LOG_TABLE': check_log_table_name,
                         'DDB_TASK_TABLE': check_task_table_name},
        )
        aurora_proxy.grant_connect(grantee=self.validate_query_function)

        # Add dynamodb event source as a trigger.
        self.validate_query_function.add_event_source(ddb_log_table_source)

        # generate report lambda function
        generate_report_function_role = iam.Role(
            self,
            "generate-report-function-role-{}".format(params['env_name']),
            role_name="generate-report-function-role-{}".format(params['env_name']),
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        s3_bucket.grant_read_write(generate_report_function_role)
        log_table.grant_read_data(generate_report_function_role)
        task_table.grant_read_write_data(generate_report_function_role)

        self.generate_report_function = aws_lambda.Function(
            self, "generate_report_function",
            code=aws_lambda.Code.from_asset("infrastructure/query_validation/lambda_function/generate_error_report"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(120),
            role=generate_report_function_role,
            function_name='db-check-generate-report-{}'.format(params['env_name'])
        )

        self.generate_report_function.add_event_source(ddb_task_table_source)

        self.generate_report_function.add_environment('BUCKET_NAME', s3_bucket.bucket_name)
        self.generate_report_function.add_environment('LOG_TABLE_NAME', log_table.table_name)
        self.generate_report_function.add_environment('TASK_TABLE_NAME', task_table.table_name)

    @property
    def validate_function(self):
        return self.validate_query_function
