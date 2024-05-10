from aws_cdk import (
    aws_ec2 as ec2,
)
from constructs import (
    Construct,
    DependencyGroup
)
from infrastructure.query_validation.aurora.stack import Aurora
from infrastructure.query_validation.lambda_function.stack import LambdaFunction
from infrastructure.shared_infrastructure import shared_infrastructure_construct


class QueryValidationConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict, vpc: ec2.Vpc, private_subnets,
                 sg: ec2.SecurityGroup, s3_bucket=shared_infrastructure_construct.Bucket,
                 dynamodb=shared_infrastructure_construct.DynamoDBTables, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        aurora = Aurora(self, "aurora_for_validation",
                        env_name=params['env_name'],
                        vpc=vpc,
                        sg=sg,
                        private_subnets=private_subnets
                        )

        lambda_function = LambdaFunction(self, 'function_for_validation', params=params,
                                         vpc=vpc,
                                         sg=sg,
                                         private_subnets=private_subnets,
                                         aurora_proxy=aurora.proxy,
                                         s3_bucket=s3_bucket.bucket,
                                         log_table=dynamodb.log_table,
                                         task_table=dynamodb.task_table,
                                         ddb_log_table_source=dynamodb.log_table_source,
                                         ddb_task_table_source=dynamodb.task_table_update_ddb_source,
                                         )

        aurora_and_lambda_group = DependencyGroup()
        aurora_and_lambda_group.add(aurora)
        aurora_and_lambda_group.add(lambda_function)
