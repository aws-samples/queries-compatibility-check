import json
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_iam as iam
)
from constructs import Construct


class StepFunctions(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env_name = params['env_name']
        region = params["region"]
        account = params["account"]
        check_task_table_name = params["check_task_table_name"]
        check_log_table_name = params["check_log_table_name"]
        asg_name = params["asg_name"]
        sqs_queue_name = "queries-compatibility-check-queue-{}".format(env_name)
        create_step_function_arn = f"arn:aws:states:{region}:{account}:execution:CreateTaskStateMachine_{env_name}"

        # Create policy document
        policy = iam.Policy(
            self,
            "StepFunctionPolicy-{}".format(env_name),
            policy_name="step-function-policy-{}".format(env_name),
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['dynamodb:PutItem',
                             'dynamodb:GetItem',
                             'dynamodb:UpdateItem',
                             'dynamodb:Query' ],
                    resources=[f'arn:aws:dynamodb:{region}:{account}:table/{check_task_table_name}',
                               f'arn:aws:dynamodb:{region}:{account}:table/{check_task_table_name}/index/*',
                               f'arn:aws:dynamodb:{region}:{account}:table/{check_log_table_name}'],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['lambda:InvokeFunction'],
                    resources=[f'{params["get_db_instance_type_function_arn"]}:*', params["get_db_instance_type_function_arn"]],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['autoscaling:UpdateAutoScalingGroup'],
                    resources=[params['asg_arn']],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['sqs:PurgeQueue'],
                    resources=[f'arn:aws:sqs:{region}:{account}:{sqs_queue_name}'],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['ec2:DeleteTrafficMirrorSession',
                             'ec2:CreateTrafficMirrorSession'],
                    resources=['*'],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['states:StopExecution'],
                    resources=['{}:*'.format(create_step_function_arn)],
                )
            ]
        )

        role = iam.Role(self,  "StepFunctionRole-{}".format(env_name),
                        role_name="step-function-role-{}".format(env_name),
                        assumed_by=iam.ServicePrincipal("states.amazonaws.com")
                        )

        role.attach_inline_policy(policy)

        cleanup_function_definition = '''
        {
          "Comment": "A description of my state machine",
          "StartAt": "DynamoDB GetItem",
          "States": {
            "DynamoDB GetItem": {
              "Type": "Task",
              "Resource": "arn:aws:states:::dynamodb:getItem",
              "Parameters": {
                "TableName": "check_task_table_name",
                "Key": {
                  "task_id": {
                    "S.$": "$.task_id"
                  }
                }
              },
              "Next": "Map",
              "ResultPath": "$.task"
            },
            "Map": {
              "Type": "Map",
              "ItemProcessor": {
                "ProcessorConfig": {
                  "Mode": "INLINE"
                },
                "StartAt": "DeleteTrafficMirrorSession",
                "States": {
                  "DeleteTrafficMirrorSession": {
                    "Type": "Task",
                    "Parameters": {
                      "TrafficMirrorSessionId.$": "$.M.session_id.S"
                    },
                    "Resource": "arn:aws:states:::aws-sdk:ec2:deleteTrafficMirrorSession",
                    "End": true
                  }
                }
              },
              "Next": "UpdateAutoScalingGroup",
              "ItemsPath": "$.task.Item.traffic_mirroring.L",
              "ResultPath": "$.mirror_sessions"
            },
            "UpdateAutoScalingGroup": {
              "Type": "Task",
              "Parameters": {
                "AutoScalingGroupName": "traffic-mirror-asg",
                "DesiredCapacity": 0,
                "MaxSize": 0,
                "MinSize": 0
              },
              "Resource": "arn:aws:states:::aws-sdk:autoscaling:updateAutoScalingGroup",
              "ResultPath": "$.update_asg",
              "Next": "PurgeQueue"
            },
            "PurgeQueue": {
              "Type": "Task",
              "Parameters": {
                "QueueUrl": "sqs_queue_url"
              },
              "Resource": "arn:aws:states:::aws-sdk:sqs:purgeQueue",
              "Next": "Is stopped manually",
              "ResultPath": "$.sqs"
            },
            "Is stopped manually": {
              "Type": "Choice",
              "Choices": [
                {
                  "Not": {
                    "Variable": "$.auto_finished",
                    "IsPresent": true
                  },
                  "Next": "DynamoDB UpdateTaskStatusStopped"
                }
              ],
              "Default": "DynamoDB UpdateTaskStatusFinished"
            },
            "DynamoDB UpdateTaskStatusStopped": {
              "Type": "Task",
              "Resource": "arn:aws:states:::dynamodb:updateItem",
              "Parameters": {
                "TableName": "check_task_table_name",
                "Key": {
                  "task_id": {
                    "S.$": "$.task_id"
                  }
                },
                "UpdateExpression": "SET #s = :stop, in_progress = :not, end_time = :end_time",
                "ExpressionAttributeNames": {
                  "#s": "status"
                },
                "ExpressionAttributeValues": {
                  ":stop": {
                    "S": "Stopped"
                  },
                  ":not": {
                    "N": "0"
                  },
                  ":end_time": {
                    "S.$": "$$.State.EnteredTime"
                  }
                }
              },
              "Next": "StopExecution",
              "ResultPath": "$.ddb_task_stopped"
            },
            "StopExecution": {
              "Type": "Task",
              "End": true,
              "Parameters": {
                "ExecutionArn.$": "States.Format('create_step_function_arn:{}', $.task_id)"
              },
              "Resource": "arn:aws:states:::aws-sdk:sfn:stopExecution"
            },
            "DynamoDB UpdateTaskStatusFinished": {
              "Type": "Task",
              "Resource": "arn:aws:states:::dynamodb:updateItem",
              "Parameters": {
                "TableName": "check_task_table_name",
                "Key": {
                  "task_id": {
                    "S.$": "$.task_id"
                  }
                },
                "UpdateExpression": "SET #s = :finish, in_progress = :not, end_time = :end_time",
                "ExpressionAttributeNames": {
                  "#s": "status"
                },
                "ExpressionAttributeValues": {
                  ":finish": {
                    "S": "Finished"
                  },
                  ":not": {
                    "N": "0"
                  },
                  ":end_time": {
                    "S.$": "$$.State.EnteredTime"
                  }
                }
              },
              "End": true
            }
          }
        }
        '''
        cleanup_function_definition = cleanup_function_definition.replace("check_task_table_name", params['check_task_table_name'])
        cleanup_function_definition = cleanup_function_definition.replace("sqs_queue_url", params['queries_queue_url'])
        cleanup_function_definition = cleanup_function_definition.replace("traffic-mirror-asg", params['asg_name'])
        cleanup_function_definition = cleanup_function_definition.replace("create_step_function_arn",
                                                                          create_step_function_arn)

        self.cleanup_step_function = sfn.CfnStateMachine(
            self,
            "CleanupTaskStateMachine_{}".format(env_name),
            state_machine_name="CleanupTaskStateMachine_{}".format(env_name),
            role_arn=role.role_arn,
            definition_string=cleanup_function_definition
        )

        create_function_definition = '''
            {
              "Comment": "A description of my state machine",
              "StartAt": "Check status",
              "States": {
                "Check status": {
                  "Type": "Task",
                  "Next": "Existing task?",
                  "Parameters": {
                    "TableName": "check_task",
                    "IndexName": "in_progress-created_time-index",
                    "Select": "COUNT",
                    "KeyConditionExpression": "in_progress = :flag",
                    "ExpressionAttributeValues": {
                      ":flag": {
                        "N": "1"
                      }
                    }
                  },
                  "Resource": "arn:aws:states:::aws-sdk:dynamodb:query",
                  "ResultPath": "$.get_status",
                  "ResultSelector": {
                    "in_progress_count.$": "$.Count",
                    "executionArn.$": "States.StringSplit($$.Execution.Id, ':')"
                  }
                },
                "Existing task?": {
                  "Type": "Choice",
                  "Choices": [
                    {
                      "Variable": "$.get_status.in_progress_count",
                      "NumericGreaterThan": 0,
                      "Next": "Task current running error"
                    }
                  ],
                  "Default": "Insert task"
                },
                "Task current running error": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "check_task",
                    "Item": {
                      "task_id": {
                        "S.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "in_progress": {
                        "N": "0"
                      },
                      "traffic_window": {
                        "N.$": "States.JsonToString($.traffic_window)"
                      },
                      "message": {
                        "S": "There is at least a task in running state."
                      },
                      "status": {
                        "S": "Error"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      }
                    }
                  },
                  "End": true
                },
                "Insert task": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "check_task",
                    "Item": {
                      "task_id": {
                        "S.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "in_progress": {
                        "N": "1"
                      },
                      "traffic_window": {
                        "N.$": "States.JsonToString($.traffic_window)"
                      },
                      "message": {
                        "S": ""
                      },
                      "status": {
                        "S": "Created"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      }
                    }
                  },
                  "Next": "Get cluster information",
                  "ResultPath": "$.insert_task",
                  "ResultSelector": {}
                },
                "Get cluster information": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::lambda:invoke",
                  "Parameters": {
                    "Payload.$": "$",
                    "FunctionName": "arn:aws:lambda:us-east-1:172814635940:function:get_rds_instance_type:$LATEST"
                  },
                  "Retry": [
                    {
                      "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.TooManyRequestsException"
                      ],
                      "IntervalSeconds": 1,
                      "MaxAttempts": 3,
                      "BackoffRate": 2
                    }
                  ],
                  "Next": "Get cluster information error?",
                  "ResultPath": "$.cluster_info",
                  "ResultSelector": {
                    "endpoint.$": "$.Payload.endpoint",
                    "read_endpoint.$": "$.Payload.read_endpoint",
                    "error.$": "$.Payload.error",
                    "instance_count.$": "$.Payload.instance_count",
                    "instances.$": "$.Payload.instances",
                    "stop_time.$": "$.Payload.stop_time"
                  }
                },
                "Get cluster information error?": {
                  "Type": "Choice",
                  "Choices": [
                    {
                      "Not": {
                        "Variable": "$.cluster_info.error",
                        "StringEquals": ""
                      },
                      "Next": "Store cluster error"
                    }
                  ],
                  "Default": "Update Auto Scaling Group"
                },
                "Store cluster error": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "check_task",
                    "Item": {
                      "task_id": {
                        "S.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "endpoint": {
                        "S.$": "$.cluster_info.endpoint"
                      },
                      "read_endpoint": {
                        "S.$": "$.cluster_info.read_endpoint"
                      },
                      "in_progress": {
                        "N": "0"
                      },
                      "traffic_window": {
                        "N.$": "States.JsonToString($.traffic_window)"
                      },
                      "instance_count": {
                        "N": "0"
                      },
                      "message": {
                        "S.$": "$.cluster_info.error"
                      },
                      "status": {
                        "S": "Error"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      }
                    }
                  },
                  "End": true
                },
                "Clear Auto Scaling Group": {
                  "Type": "Task",
                  "Next": "Store traffic mirror error",
                  "Parameters": {
                    "AutoScalingGroupName": "traffic-mirror-asg",
                    "DesiredCapacity": 0,
                    "MaxSize": 0,
                    "MinSize": 0
                  },
                  "Resource": "arn:aws:states:::aws-sdk:autoscaling:updateAutoScalingGroup",
                  "ResultPath": "$.clear_asg"
                },
                "Store traffic mirror error": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "check_task",
                    "Item": {
                      "task_id": {
                        "S.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "endpoint": {
                        "S.$": "$.cluster_info.endpoint"
                      },
                      "read_endpoint": {
                        "S.$": "$.cluster_info.read_endpoint"
                      },
                      "in_progress": {
                        "N": "0"
                      },
                      "traffic_window": {
                        "N.$": "States.JsonToString($.traffic_window)"
                      },
                      "instance_count": {
                        "N.$": "States.JsonToString($.cluster_info.instance_count)"
                      },
                      "message": {
                        "S.$": "$.create_tms.error.Cause"
                      },
                      "status": {
                        "S": "Error"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      },
                      "instances": {
                        "L.$": "$.cluster_info.instances"
                      }
                    }
                  },
                  "End": true
                },
                "Update Auto Scaling Group": {
                  "Type": "Task",
                  "Next": "Iterate eni",
                  "Parameters": {
                    "AutoScalingGroupName": "traffic-mirror-asg",
                    "DesiredCapacity.$": "$.cluster_info.instance_count",
                    "MaxSize.$": "$.cluster_info.instance_count",
                    "MinSize.$": "$.cluster_info.instance_count"
                  },
                  "Resource": "arn:aws:states:::aws-sdk:autoscaling:updateAutoScalingGroup",
                  "ResultPath": "$.update_asg",
                  "Catch": [
                    {
                      "ErrorEquals": [
                        "States.ALL"
                      ],
                      "Next": "Store asg error",
                      "ResultPath": "$.update_asg.error"
                    }
                  ]
                },
                "Iterate eni": {
                  "Type": "Map",
                  "ItemProcessor": {
                    "ProcessorConfig": {
                      "Mode": "INLINE"
                    },
                    "StartAt": "Create Traffic Mirror Session",
                    "States": {
                      "Create Traffic Mirror Session": {
                        "Type": "Task",
                        "Parameters": {
                          "NetworkInterfaceId.$": "$.M.eni.M.eni_id.S",
                          "SessionNumber": 3,
                          "TrafficMirrorFilterId": "tmf-0766c54e75fe865d8",
                          "TrafficMirrorTargetId": "tmt-0bfe598d01bcd1905",
                          "VirtualNetworkId": 9804898
                        },
                        "Resource": "arn:aws:states:::aws-sdk:ec2:createTrafficMirrorSession",
                        "End": true,
                        "ResultSelector": {
                          "M": {
                            "filter_id": {
                              "S.$": "$.TrafficMirrorSession.TrafficMirrorFilterId"
                            },
                            "session_id": {
                              "S.$": "$.TrafficMirrorSession.TrafficMirrorSessionId"
                            },
                            "target_id": {
                              "S.$": "$.TrafficMirrorSession.TrafficMirrorTargetId"
                            },
                            "virtual_network": {
                              "N.$": "States.JsonToString($.TrafficMirrorSession.VirtualNetworkId)"
                            }
                          }
                        }
                      }
                    }
                  },
                  "Next": "Store task",
                  "ItemsPath": "$.cluster_info.instances",
                  "Catch": [
                    {
                      "ErrorEquals": [
                        "States.ALL"
                      ],
                      "Next": "Clear Auto Scaling Group",
                      "ResultPath": "$.create_tms.error"
                    }
                  ],
                  "ResultPath": "$.batch_create_tms"
                },
                "Store task": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "check_task",
                    "Item": {
                      "task_id": {
                        "S.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "endpoint": {
                        "S.$": "$.cluster_info.endpoint"
                      },
                      "read_endpoint": {
                        "S.$": "$.cluster_info.read_endpoint"
                      },
                      "in_progress": {
                        "N": "1"
                      },
                      "captured_query": {
                        "N": "0"
                      },
                      "checked_query": {
                        "N": "0"
                      },
                      "failed_query": {
                        "N": "0"
                      },
                      "traffic_window": {
                        "N.$": "States.JsonToString($.traffic_window)"
                      },
                      "instance_count": {
                        "N.$": "States.JsonToString($.cluster_info.instance_count)"
                      },
                      "message": {
                        "S": ""
                      },
                      "status": {
                        "S": "Created"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      },
                      "traffic_mirroring": {
                        "L.$": "$.batch_create_tms"
                      },
                      "instances": {
                        "L.$": "$.cluster_info.instances"
                      }
                    }
                  },
                  "Next": "Wait",
                  "ResultPath": "$.insert_task"
                },
                "Store asg error": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "check_task",
                    "Item": {
                      "task_id": {
                        "S.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "writer_class": {
                        "S.$": "$.cluster_info.writer_class"
                      },
                      "endpoint": {
                        "S.$": "$.cluster_info.endpoint"
                      },
                      "writer_identifier": {
                        "S.$": "$.cluster_info.writer_identifier"
                      },
                      "read_endpoint": {
                        "S.$": "$.cluster_info.read_endpoint"
                      },
                      "in_progress": {
                        "N": "0"
                      },
                      "traffic_window": {
                        "N.$": "States.JsonToString($.traffic_window)"
                      },
                      "instance_count": {
                        "N.$": "States.JsonToString($.cluster_info.instance_count)"
                      },
                      "message": {
                        "S.$": "$.update_asg.error.Cause"
                      },
                      "status": {
                        "S": "Error"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      },
                      "instances": {
                        "L.$": "$.cluster_info.instances"
                      }
                    }
                  },
                  "End": true
                },
                "Wait": {
                  "Type": "Wait",
                  "Next": "Clear resources",
                  "Comment": "$.cluster_info.stop_time",
                  "TimestampPath": "$.cluster_info.stop_time"
                },
                "Clear resources": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::states:startExecution",
                  "Parameters": {
                    "StateMachineArn": "arn:aws:states:us-east-1:172814635940:stateMachine:MyStateMachine-vgws3brl5",
                    "Input": {
                      "auto_finished": 1,
                      "task_id.$": "States.ArrayGetItem($.get_status.executionArn, 7)"
                    }
                  },
                  "End": true
                }
              }
            }
        '''

        create_function_definition_dict = json.loads(create_function_definition)

        create_function_definition_dict['States']['Get cluster information']['Parameters']['FunctionName'] = '{}:$LATEST'.format(params['get_db_instance_type_function_arn'])

        create_function_definition_dict['States']['Check status']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['Check status']['Parameters']['IndexName'] = params['check_task_table_gsi_name']
        create_function_definition_dict['States']['Task current running error']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['Insert task']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['Store cluster error']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['Store task']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['Store traffic mirror error']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['Store asg error']['Parameters']['TableName'] = params['check_task_table_name']

        create_function_definition_dict['States']['Iterate eni']['ItemProcessor']['States']['Create Traffic Mirror Session']['Parameters']['TrafficMirrorFilterId'] = params['tmf_id']
        create_function_definition_dict['States']['Iterate eni']['ItemProcessor']['States']['Create Traffic Mirror Session']['Parameters']['TrafficMirrorTargetId'] = params['tmt_id']

        create_function_definition_dict['States']['Update Auto Scaling Group']['Parameters']['AutoScalingGroupName'] = params['asg_name']
        create_function_definition_dict['States']['Clear Auto Scaling Group']['Parameters']['AutoScalingGroupName'] = params['asg_name']

        create_function_definition_dict['States']['Clear resources']['Parameters']['StateMachineArn'] = self.cleanup_step_function.attr_arn

        self.create_step_function = sfn.CfnStateMachine(
            self,
            "CreateTaskStateMachine_{}".format(env_name),
            state_machine_name="CreateTaskStateMachine_{}".format(env_name),
            role_arn=role.role_arn,
            definition_string=json.dumps(create_function_definition_dict)
        )

        execution_policy = iam.Policy(
            self,
            "StepFunctionExecutionPolicy-{}".format(env_name),
            policy_name="step-function-execution-policy-{}".format(env_name),
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['states:StartExecution'],
                    resources=[self.cleanup_step_function.attr_arn],
                )
            ]
        )

        role.attach_inline_policy(execution_policy)

