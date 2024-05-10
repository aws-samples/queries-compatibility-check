import json
import boto3
import re
import dns.resolver
from datetime import datetime, timedelta

ec2_client = boto3.client('ec2')
rds = boto3.client('rds')


def get_ip_for_database_endpoint(endpoint):
    answers = dns.resolver.query(endpoint)
    for rdata in answers:
        ip = str(rdata)
        return ip


def calculate_instance_count_by_db_class(instance_class):
    class_parts = instance_class.split('.')[-1]
    pattern = r'^\d+'
    result = re.match(pattern, class_parts)
    if result:
        return int(int(result.group())/2)
    else:
        return 1


def get_eni_for_ip(ip):
    
    eni = {
        'eni_id': {
            'S': ''
        },
        'subnet_id': {
            'S': ''
        }
    }
    
    response = ec2_client.describe_network_interfaces(
        Filters=[
            {
                'Name': 'addresses.private-ip-address',
                'Values': [
                    ip,
                ]
            },
        ],
        DryRun=False,
    )
    
    if len(response['NetworkInterfaces']) > 0:
        
        eni['eni_id']['S'] = response['NetworkInterfaces'][0]['NetworkInterfaceId']
        eni['subnet_id']['S']  = response['NetworkInterfaces'][0]['SubnetId']
        
        return eni
        
    return None


def lambda_handler(event, context):
    
    ip_list = []
    
    database_info = {
        'endpoint': '',
        'read_endpoint': '',
        'instance_count': 0,
        'error': '',
        'instances': []
    }
    
    cluster_identifier = event['cluster_identifier']
    traffic_window = event['traffic_window']
    
    # Describe the database cluster
    try:
        response = rds.describe_db_clusters(
            DBClusterIdentifier=cluster_identifier
        )
        
    except Exception as e:
        print(e)
        database_info['error'] = 'endpoint error found'
        
        return database_info
    
    cluster = response['DBClusters'][0]
    
    database_info['endpoint'] = cluster['Endpoint']
    database_info['read_endpoint'] = cluster['ReaderEndpoint']
    
    instances_info = rds.describe_db_instances(
        Filters=[
            {
                'Name': 'db-cluster-id',
                'Values': [
                    cluster_identifier,
                ]
            },
        ],
    )
    
    for instance_info in instances_info['DBInstances']:
        instance = {
            'M': {
                'identifier': {
                    'S': instance_info['DBInstanceIdentifier']
                },
                'class': {
                    'S': instance_info['DBInstanceClass']
                },
                'endpoint': {
                    'S': instance_info['Endpoint']['Address']
                },
            }
        }
        
        ip = get_ip_for_database_endpoint(instance['M']['endpoint']['S'])
        instance['M']['ip'] = { 'S': ip }
        
        instance['M']['eni'] = { 'M': get_eni_for_ip(ip) }
        
        database_info['instances'].append(instance)
        database_info['instance_count'] = database_info['instance_count'] + calculate_instance_count_by_db_class(instance['M']['class']['S'])
        
    now = datetime.now()
    stop_time = now + timedelta(hours=traffic_window)
    iso_time_with_utc_offset = '{}+00:00'.format(stop_time.isoformat())        
    
    database_info['stop_time'] = iso_time_with_utc_offset
    
    return database_info

