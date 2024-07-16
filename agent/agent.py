import subprocess
import boto3
import json
import re
import logging
import logging.config
import configparser
from hashlib import blake2b
from boto3.dynamodb.conditions import Key, Attr
from multiprocessing import Pool
import sys
import traceback
from datetime import datetime


command = [
                'sudo',
                'tshark',
                '-i',
                'capture0',
                '-T',
                'fields',
                '-e',
                'frame.time_epoch',
                '-e',
                'ip.src',
                '-e',
                'tcp.srcport',
                '-e',
                'mysql.command',
                '-e',
                'mysql.query',
                '-e',
                'mysql.field.type',
                '-Y',
                '(mysql.command==3 or mysql.command==22) and mysql and tcp.srcport!=3306',
                # '(mysql.command==3 or mysql.command==22 or mysql.command==23) and mysql and tcp.srcport!=3306',
                '-l'
            ]


def read_config(path):
    """
    Initialize the global configuration file.
    @param path Configuration file address
    @return Configuration file DICT object"
    """
    logger = logging.getLogger(__name__)
    config = configparser.ConfigParser()
    config.read(path)
    logger.debug("Configuration is parsed: %s", {section: dict(
        config[section]) for section in config.sections()})
    return config


# Global variables
config = read_config('/home/ec2-user/agent/config.conf')

region = config.get('DEFAULT', 'region')
queue_url = config.get('DEFAULT', 'queue_url')
table_name = config.get('DEFAULT', 'task_dynamodb_name')

sessions = {}
sqs_client = boto3.client('sqs', region_name=region)
sqs_url = queue_url
task_id = ''

# get current task id
dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table(table_name)

partition_key_value = 1

response = table.query(
    KeyConditionExpression=Key('in_progress').eq(1),
    IndexName='in-progress-time-index'
)

if 'Items' in response:
    if len(response['Items']) > 0:
        task_id = response['Items'][0]['task_id']
        print('Task ID: {}'.format(task_id))
    else:
        print('No task found')
        sys.exit(0)
else:
    print('No task found')
    sys.exit(0)

pattern = r"'[^']*'"


def start_query_prepare_session(event):
    key = '{}:{}'.format(event['src'], event['src_port'])
    query = event['query']
    sessions[key] = query

    
def replace_query_parameters(query, parameters):
    full_stmt = query.replace('?', '%s') % tuple(parameters)
    # remove the new line in the full_stmt
    return full_stmt


def end_session(event):
    key = '{}:{}'.format(event['src'], event['src_port'])
    if key in sessions:
        # go through data type
        print(event['params'])
        print(type(event['params']))
        
        params_types = event['params'].split(",")

        params = []

        for params_type in params_types:
            if params_type == '8':
                params.append(1)
            else:
                params.append("''")

        event['query'] = replace_query_parameters(sessions[key], params)
        del sessions[key]
        send_command_to_queue(event)


def replace_all_placeholder_as_empty_string_value(event):
    event['query'] = event['query'].replace('?', "''")
    send_command_to_queue(event)

def remove_sql_comments(sql):
    # Remove -- style comments
    sql = re.sub(r'--.*?(\n|$)', '\n', sql)
    
    # Remove # style comments
    sql = re.sub(r'#.*?(\n|$)', '\n', sql)
    
    # Remove /* */ style comments
    sql = re.sub(r'/\*[\s\S]*?\*/', '', sql)
    
    # Remove extra whitespace
    sql = re.sub(r'\s+', ' ', sql).strip()
    
    return sql

def replace_numbers_with_one(sql):
    # Regular expression pattern to match integers and floats
    pattern = r'\b\d+(\.\d+)?\b'
    
    # Replace all matches with '1'
    result = re.sub(pattern, '1', sql)
    
    return result

def send_command_to_queue(event):

    # remove special characters, like new line.
    event['query'] = event['query'].replace('\\n', ' ').replace('\\t', ' ').replace('\\r', ' ')

    # remove comments
    event['query'] = remove_sql_comments(event['query'])

    # remove all digits
    event['query'] = replace_numbers_with_one(event['query'])

    # replit multiple queries
    queries = event['query'].split(';')

    for query in queries:
        if query.strip() != '':
            event['query'] = query
            event['query_hash'] = blake2b(event['query'].encode()).hexdigest()
            sqs_client.send_message(
                QueueUrl=sqs_url,
                MessageBody=json.dumps(event),
            )

    # event['query_hash'] = blake2b(event['query'].encode()).hexdigest()
    # event['query'] = event['query'].replace('\\n', ' ').replace('\\t', ' ').replace('\\r', ' ')

    # sqs_client.send_message(
    #     QueueUrl=sqs_url,
    #     MessageBody=json.dumps(event),
    # )


def process_output(output):
    output_string_array = output.strip().decode('utf-8').split('\t')

    result = {
                'task_id': task_id,
                'time': output_string_array[0],
                'src': output_string_array[1],
                'src_port': output_string_array[2],
                'command': output_string_array[3],
                'query': '',
                'params': '',
            }
    
    if result['command'] == '3':
        result['query'] = re.sub(pattern, "''", output_string_array[4])
        send_command_to_queue(result)
            
    if result['command'] == '22':
        result['query'] = re.sub(pattern, "''", output_string_array[4])
        replace_all_placeholder_as_empty_string_value(result)


def run_command():
    process = subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=0)
    pool = Pool(processes=7)

    while True:
        try:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                pool.apply_async(process_output, (output,))

        except Exception as e:
            print(e)
            error_traceback = traceback.format_exc()
            print(error_traceback)
            break

    pool.close()
    pool.join()


if __name__ == '__main__':
    run_command()

