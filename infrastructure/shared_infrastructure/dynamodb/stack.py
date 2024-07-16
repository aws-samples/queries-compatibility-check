from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_lambda_event_sources as source,
    aws_lambda
)
from constructs import Construct


class DynamoDBTables(Construct):
    def __init__(self, scope: Construct, construct_id: str,
                 env_name: str, task_table: str, log_table: str, task_table_gsi: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Check task table

        gsi_name =task_table_gsi
        gsi_partition_key = dynamodb.Attribute(
            name='in_progress',
            type=dynamodb.AttributeType.NUMBER
        )
        gsi_sort_key = dynamodb.Attribute(
            name='created_time',
            type=dynamodb.AttributeType.STRING
        )
        gsi = dynamodb.GlobalSecondaryIndexProps(
            index_name=gsi_name,
            partition_key=gsi_partition_key,
            sort_key=gsi_sort_key,
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=['task_id']
        )

        self.task_table = dynamodb.Table(
            self, "check_task_table",
            table_name=task_table,
            partition_key=dynamodb.Attribute(name="task_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True,
        )

        self.task_table.add_global_secondary_index(
            index_name=gsi_name,
            partition_key=gsi_partition_key,
            sort_key=gsi_sort_key,
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=['task_id']
        )

        # task table stream
        self.task_table_source = source.DynamoEventSource(
            self.task_table,
            retry_attempts=1,
            batch_size=1,
            starting_position=aws_lambda.StartingPosition.LATEST,
            filters=[aws_lambda.FilterCriteria.filter(
                {
                    "dynamodb": {"NewImage": {"status": {"S": ["STOPPED", "FINISHED"]}}},
                    "eventName": aws_lambda.FilterRule.is_equal("MODIFY")
                }
            )]
        )

        # Check log table
        self.log_table = dynamodb.Table(
            self, "check_log_table",
            table_name=log_table,
            partition_key=dynamodb.Attribute(name="task_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="query_hash", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True
        )

        # log table stream, update
        self.log_table_source = source.DynamoEventSource(
            self.log_table,
            retry_attempts=1,
            batch_size=50,
            starting_position=aws_lambda.StartingPosition.LATEST,
            filters=[aws_lambda.FilterCriteria.filter({"eventName": aws_lambda.FilterRule.is_equal("INSERT")})]
        )
        
        # task table stream, use to trigger generate report lambda function when the task is stopped or finished.
        self.task_table_update_ddb_source = source.DynamoEventSource(
            self.task_table,
            retry_attempts=1,
            batch_size=1,
            starting_position=aws_lambda.StartingPosition.LATEST,
            filters=[aws_lambda.FilterCriteria.filter(
                { 
                    "dynamodb": { "NewImage": { "status": { "S": ["Stopped", "Finished"]} } }, 
                    "eventName": aws_lambda.FilterRule.is_equal("MODIFY") 
                }
            )]
        )

