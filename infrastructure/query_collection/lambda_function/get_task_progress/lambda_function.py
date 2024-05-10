import boto3
import os
import json
import logging
from enum import Enum
import traceback
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Task(Enum):
    CREATED = "Created"
    STOPPED = "Stopped"
    FINISHED = "Finished"
    IN_PROGRESS = "In-progress"
    ERROR = "Error"


class QueryLog(Enum):
    CREATED = "Created"
    FAILED = "Failed"
    CHECKED = "Checked"


REGION = os.environ.get("REGION")

# S3 client
s3 = boto3.client('s3')

# dynamodb client
dynamodb = boto3.resource("dynamodb", region_name=REGION)
task_table_name = os.environ.get("DDB_TASK_TABLE")
task_table = dynamodb.Table(task_table_name)


def get_task_complete_percentage(create_time: str, traffic_window: int):
    """
        Calculates the completion percentage of a task based on its creation time and traffic window duration.

        Args:
            create_time (str): The creation time of the task in the format '%Y-%m-%dT%H:%M:%S.%f%z'.
            traffic_window (int): The duration of the traffic window in hours.

        Returns:
            str: The completion percentage formatted as a string with two decimal places, followed by '%'.
                 If the task has exceeded the traffic window duration, it returns '100%'.
    """

    traffic_window_seconds = traffic_window * 3600
    create_datetime = datetime.strptime(create_time, '%Y-%m-%dT%H:%M:%S.%f%z')

    current_time = datetime.now()
    diff_seconds = int(current_time.timestamp() - create_datetime.timestamp())

    if traffic_window_seconds >= diff_seconds:
        percentage = (diff_seconds / traffic_window_seconds) * 100
        # Format to a percentage str.
        return f"{percentage:.2f}%"
    else:
        return "100%"


def get_task_info(task_id: str):
    """
        Retrieves information about a task from the DynamoDB table.

        Args:
            task_id (str): The ID of the task to retrieve information for.

        Returns:
            dict: A dictionary containing information about the task, including its status, progress, and report details.
    """

    return_dict = {"message": ""}
    key = {
        "task_id": task_id
    }
    try:
        response = task_table.get_item(
            Key=key
        )
        if "Item" in response:
            item = response["Item"]
            status = item["status"]
            if status == Task.ERROR.value:
                return {"message": item["message"]}

            return_dict["status"] = status
            return_dict["captured_query"] = int(item["captured_query"])
            return_dict["checked_query"] = int(item["checked_query"])
            return_dict["failed_query"] = int(item["failed_query"])
            return_dict["message"] = item["message"]
            return_dict["created_time"] = item["created_time"]
            return_dict["traffic_window"] = int(item["traffic_window"])
            return_dict["complete_percentage"] = get_task_complete_percentage(item["created_time"],
                                                                              int(item["traffic_window"]))
            if "start_capture_time" in item:
                return_dict["start_capture_time"] = item["start_capture_time"]

            if status == Task.STOPPED.value or status == Task.FINISHED.value:
                if "end_time" in item:
                    return_dict["end_time"] = item["end_time"]
                if item.get("report_s3_bucket") and item.get("report_s3_key"):
                    return_dict["complete_percentage"] = "100%"
                    report_s3_uri = "s3://" + item["report_s3_bucket"] + "/" + item["report_s3_key"]
                    try:
                        response = s3.generate_presigned_url('get_object', Params={
                                'Bucket': item["report_s3_bucket"],
                                'Key': item["report_s3_key"]
                            }, ExpiresIn=172800
                        )
                        return_dict["report_s3_presign_url"] = response
                    except ClientError as e:
                        logger.error(e)
                else:
                    report_s3_uri = "Report is generating, please wait a moment."
                return_dict["report_s3_uri"] = report_s3_uri
        else:
            return_dict["message"] = "The task_id is not in DynamoDB table."
    except Exception as e:
        logger.error("Get check_task item failed! key = " + str(key))
        error_traceback = traceback.format_exc()
        logger.error(error_traceback)
        return_dict["message"] = str(e)

    return return_dict


def lambda_handler(event, context):
    if event.get("queryStringParameters") and event["queryStringParameters"].get("task_id"):
        resp_body = get_task_info(event["queryStringParameters"]["task_id"])
    else:
        resp_body = {"message": "Please input task_id."}
    return {
        "statusCode": 202,  # Custom success code (optional)
        "body": json.dumps(resp_body)
    }
