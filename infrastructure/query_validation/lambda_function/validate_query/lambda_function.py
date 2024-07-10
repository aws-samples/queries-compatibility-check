import pymysql
import boto3
import os
import re
import json
import logging
from enums import Task, QueryLog

logger = logging.getLogger()
logger.setLevel(logging.INFO)


REGION = os.environ.get("REGION")
ENDPOINT = os.environ.get("PROXY_ENDPOINT")

PORT = 3306
USER = "admin"
DBNAME = "mysql"
pattern = r"'[^']*'"

client = boto3.client('rds')

query_command = 'SELECT STATEMENT_DIGEST_TEXT(%s)'
token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

# dynamodb client
dynamodb = boto3.resource('dynamodb', region_name=REGION)
log_table_name = os.environ.get("DDB_LOG_TABLE")
log_table = dynamodb.Table(log_table_name)

task_table_name = os.environ.get("DDB_TASK_TABLE")
task_table = dynamodb.Table(task_table_name)

# Create database connection
conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=token, port=PORT, database=DBNAME,
                       ssl_ca='global-bundle.pem')


def check_for_unsupported_functions(query):
    """
    Checks a MySQL query for potential unsupported functions using a regular expression.

    Args:
        query: The MySQL query string to be analyzed.

    Returns:
        A list of unsupported functions found in the query, or an empty list if none are found.
    """
    functions = ['load_file', 'udf', 'geometrycollectome', 'geomcollfromtext',
                 'linestringfromtext', 'polygonfromtext', 'pointfromtex', 'json_append',
                 'encode', 'decode', 'encrypt', 'des_encrypt', 'des_decrypt', 'glength']
  
     function_pattern = re.compile(r'\b(' + '|'.join(map(re.escape, functions)) + r')\s*\(', re.IGNORECASE)

    matches = function_pattern.findall(query)
    return matches


def check_for_keywords(query):
    """
        Checks if a given SQL query contains any MySQL keywords related to window functions.

        Args:
            query (str): The SQL query to be checked.

        Returns:
            list: A list of keywords found in the query.
    """
    keywords = [
        'cume_dist', 'dense_rank', 'empty', 'except', 'first_value',
        'grouping', 'groups', 'json_table', 'lag', 'last_value',
        'lateral', 'lead', 'nth_value', 'ntile', 'of', 'over',
        'percent_rank', 'rank', 'recursive', 'row_number', 'system', 'window'
    ]
    keywords_pattern = re.compile(rf"(?i)(?<!`)\b({'|'.join(map(re.escape, keywords))})\b(?!`)")
    matches = keywords_pattern.findall(query)
    return matches


def check_for_mysql_syntax(query):
    """
        Executes a given MySQL query and returns the results.

        Args:
            query (str): The MySQL query to be executed.

        Returns:
            list: A list containing the results of the executed query.
    """
    cur = conn.cursor()
    cur.execute(query_command, query)
    query_results = cur.fetchall()
    return query_results


def replace_strings(match):
    return "*" * len(match.group())


def update_log_table(log_item: dict):
    """
        Updates a log item in a DynamoDB table with the provided status and message.

        Args:
            log_item (dict): A dictionary containing the log item details, including the key, status, and message.

        Returns:
            None
    """

    # Define the update expression, attribute names, and values
    update_expression = 'SET #stat = :value, #msg = :msg_value'
    expression_attribute_names = {
        '#stat': 'status',
        '#msg': 'message'
    }
    expression_attribute_values = {
        ':value': log_item['status'],
        ':msg_value': log_item['message']
    }

    # Update the check log item.
    try:
        response = log_table.update_item(
            Key=log_item['key'],
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='NONE'
        )
    except Exception as e:
        logger.error("Update check_log item failed! key = " + str(log_item['key']))
        logger.error(e)


def update_task_table(task_id: str, checked_count: int, failed_count: int):
    """
        Updates the checked_query and failed_query counts of a task item in a DynamoDB table.

        Args:
            task_id (str): The ID of the task item to be updated.
            checked_count (int): The number of queries to increment the checked_query count by.
            failed_count (int): The number of queries to increment the failed_query count by.

        Returns:
            None
    """

    # Define the key of the item to update.
    key = {
        'task_id': task_id,
    }

    update_expression = '''SET checked_query = checked_query + :check_value,
     failed_query = failed_query + :fail_value'''

    expression_attribute_values = {
        ':check_value': checked_count,
        ':fail_value': failed_count
    }

    # Update the task item.
    try:
        response = task_table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='NONE'
        )
    except Exception as e:
        logger.error("Update check_task item failed! key = " + str(key))
        logger.error(e)


def update_task(task_id: str, log_items: list):
    """
        Updates the task and log tables with the provided log items.

        Args:
            task_id (str): The ID of the task to be updated.
            log_items (list): A list of dictionaries representing the log items.

        Returns:
            None
    """

    checked_count = 0
    failed_count = 0
    for log_item in log_items:
        checked_count += 1
        if log_item['status'] == QueryLog.FAILED.value:
            failed_count += 1
        update_log_table(log_item)
    update_task_table(task_id, checked_count, failed_count)


def update_validate_result(update_tasks: dict):
    """
        Updates the task and log tables with the provided log items for each task.

        Args:
            update_tasks (dict): A dictionary where keys are task IDs and values are lists of log items.

        Returns:
            None
    """

    for task_id, log_items in update_tasks.items():
        key = {
            "task_id": task_id
        }
        try:
            response = task_table.get_item(
                Key=key
            )
            if 'Item' in response:
                item = response['Item']
                task_status = item['status']
                # Do not update checked and failed count if a task status is Finished or Stopped.
                if task_status == Task.STOPPED.value or task_status == Task.FINISHED.value:
                    continue
                else:
                    update_task(task_id, log_items)

        except Exception as e:
            logger.error("Get check_task item failed! key = " + str(key))
            logger.error(e)


def lambda_handler(event, context):
    ddb_records = event['Records']
    update_task_dict = {}

    for record in ddb_records:
        if record['eventName'] != 'INSERT':
            continue
        log_item = record['dynamodb']['NewImage']

        task_id = log_item['task_id']['S']
        query = log_item['query']['S']
        query_hash = log_item['query_hash']['S']

        status = QueryLog.CHECKED.value
        message = ""

        unsupported_functions = check_for_unsupported_functions(query)
        keywords = check_for_keywords(query)

        if unsupported_functions:
            status = QueryLog.FAILED.value
            message = 'Query contains unsupported functions: {}; '.format(unsupported_functions)
        if keywords:
            status = QueryLog.FAILED.value
            message = message + 'Query contains 8.0 keywords without ``: {}; '.format(keywords)

        try:
            check_for_mysql_syntax(query)
        except Exception as e:
            status = QueryLog.FAILED.value
            message = message + str(e)

        # Define the key of the item to update
        key = {
            'task_id': task_id,
            'query_hash': query_hash
        }

        log_item_dict = {
            "key": key,
            "message": message,
            "status": status
        }

        if task_id in update_task_dict:
            update_task_dict[task_id].append(log_item_dict)
        else:
            update_task_dict[task_id] = [log_item_dict]

    update_validate_result(update_task_dict)

    return {
        'statusCode': 202,  # Custom success code (optional)
        'body': json.dumps('Validate query successfully!')
    }
