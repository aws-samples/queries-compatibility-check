from constructs import Construct
from aws_cdk import (
    Aws,
    Stack,
)
try:
    from aws_cdk import core as cdk
except ImportError:
    import aws_cdk as cdk

from infrastructure import stack_input
from infrastructure.query_collection.query_collection_construct import QueryCollectionConstruct
from infrastructure.shared_infrastructure.shared_infrastructure_construct import SharedInfrastructureConstruct
from infrastructure.query_validation.query_validation_construct import QueryValidationConstruct


class QueriesCompatibilityCheckStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        stack_name = '{}-{}'.format(construct_id, stack_input.env_name)
        super().__init__(scope, stack_name, **kwargs)

        # Parameters for CDK
        region = Aws.REGION
        account = Aws.ACCOUNT_ID

        # env_name = cdk.CfnParameter(self, "env", type="String")
        # vpc_id = cdk.CfnParameter(self, "vpc", type="String")

        params = {
            'region': region,
            'account': account,
            'env_name': stack_input.env_name,
            'vpc_id': stack_input.vpc_id,
            'private_subnet_ids': stack_input.private_subnet_ids,
            'public_subnet_ids': stack_input.public_subnet_ids,
            'keypair': stack_input.keypair,
            'check_task_table_name': 'check-task-table-{}'.format(stack_input.env_name),
            'check_log_table_name': 'check-log-table-{}'.format(stack_input.env_name),
            'check_task_table_gsi_name': 'in-progress-time-index'
        }

        shared_infrastructure = SharedInfrastructureConstruct(self, "SharedInfrastructureConstruct", params=params)

        query_collection = QueryCollectionConstruct(self, "QueryCollectionConstruct", 
                                                    params=params, bucket=shared_infrastructure.s3_bucket, api=shared_infrastructure.api.api, 
                                                    dynamodb_tables=shared_infrastructure.dynamodb)

        query_validation = QueryValidationConstruct(self, "QueryValidationConstruct",
                                                    params=params,
                                                    vpc=query_collection.vpc,
                                                    private_subnets=query_collection.private_subnets,
                                                    sg=query_collection.security_group,
                                                    s3_bucket = shared_infrastructure.s3_bucket,
                                                    dynamodb = shared_infrastructure.dynamodb,
                                                    )
