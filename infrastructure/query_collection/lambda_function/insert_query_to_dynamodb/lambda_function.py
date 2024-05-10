import json
import boto3
import os
from botocore.exceptions import ClientError
from enum import Enum
from datetime import datetime

REGION = os.environ.get("REGION")

dynamodb = boto3.resource('dynamodb', region_name=REGION)
task_table_name = os.environ.get("DDB_TASK_TABLE")
task_table = dynamodb.Table(task_table_name)

log_table_name = os.environ.get("DDB_LOG_TABLE")
log_table = dynamodb.Table(log_table_name)


class Task(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    FINISHED = 'Finished'
    IN_PROGRESS = 'In-progress'
    ERROR = 'Error'


def lambda_handler(event, context):
    unique_hash_dict = {}
    query_count = 0
    task_id = ''
        
    for record in event['Records']:
        query_count = query_count + 1
        body = json.loads(record['body'])
        task_id = body['task_id']

        if body['query_hash'] not in unique_hash_dict:
            unique_hash_dict[body['query_hash']] = ""
            
            # Check if the item already exists in DynamoDB
            response = log_table.get_item(
                Key={
                    'task_id': body['task_id'],
                    'query_hash': body['query_hash']
                },
                ProjectionExpression='task_id'
            )
            
            if 'Item' not in response:
                log_table.put_item(Item=body)
    
    print('*'*20 + str(query_count))

    try:
        task_key = {'task_id': task_id}
        update_expression = "set captured_query = captured_query + :captured_query, #status = :s"
        expression_attribute_values = {
            ':captured_query': query_count,
            ':s': Task.IN_PROGRESS.value,
            ':in_progress_flag': 1
        }

        response = task_table.get_item(
            Key=task_key
        )
        if "Item" in response:
            item = response["Item"]
            status = item["status"]
            if status == Task.CREATED.value:
                # The first time to capture query.
                update_expression = update_expression + ", start_capture_time = :c"
                current_time = datetime.utcnow()
                formatted_time = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                expression_attribute_values[':c'] = formatted_time

        task_table.update_item(
            Key=task_key,
            UpdateExpression=update_expression,
            ConditionExpression='in_progress = :in_progress_flag',
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='NONE'
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print('The task is stopped or finished.')
            # Handle the conditional check failure here
            # For example, you can retry the update operation
        else:
            print('Error updating item:', e)    
    
    # print(response)
    

