import boto3
import os
import csv
from enum import Enum


class Task(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    FINISHED = 'Finished'
    IN_PROGRESS = 'In-progress'
    ERROR = 'Error'


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
log_table = dynamodb.Table(os.environ['LOG_TABLE_NAME'])
task_table = dynamodb.Table(os.environ['TASK_TABLE_NAME'])
bucket_name = os.environ['BUCKET_NAME']


def update_task_db(task_id, report_s3_key):
    task_table.update_item(
        Key={'task_id': task_id},
        UpdateExpression="set report_s3_key = :r, report_s3_bucket = :b",
        ExpressionAttributeValues={
            ':r': report_s3_key,
            ':b': bucket_name
        }
    )


def get_failed_items(task_id):
    csv_items = []

    response = log_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('task_id').eq(task_id),
        FilterExpression=boto3.dynamodb.conditions.Attr('status').eq('Failed'),
        ProjectionExpression='task_id, #query, src, src_port, message',
        ExpressionAttributeNames={
            '#query': 'query',
        },
    )

    items = response['Items']
    for item in items:
        csv_item = [task_id, item['query'].replace("\"", ""), item['src'],
                    item['src_port'], item['message'].replace("\"", "")]
        csv_items.append(csv_item)

    while 'LastEvaluatedKey' in response:
        response = log_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('task_id').eq(task_id),
            FilterExpression=boto3.dynamodb.conditions.Attr('status').eq('Failed'),
            ProjectionExpression='task_id, #query, src, src_port, message',
            ExpressionAttributeNames={
                '#query': 'query',
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items = response['Items']
        for item in items:
            csv_item = [task_id, item['query'].replace("\"", ""), item['src'],
                        item['src_port'], item['message'].replace("\"", "")]
            csv_items.append(csv_item)

    return csv_items


def lambda_handler(event, context):
    
    record = event['Records'][0]

    task_item = record['dynamodb']['NewImage']

    print(task_item)

    task_id = task_item['task_id']['S']
    status = task_item['status']['S']

    # Only process STOPPED or FINISHED event
    if status == Task.STOPPED.value or status == Task.FINISHED.value:
        failed_items = get_failed_items(task_id=task_id)

        failed_items_key = 'failed_reports/id={}/failed_queries.csv'.format(task_id)
        
        # write item to csv file
        with open('/tmp/failed_queries.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerows(failed_items)

        s3.upload_file('/tmp/failed_queries.csv', bucket_name, failed_items_key)

        update_task_db(task_id=task_id, report_s3_key=failed_items_key)