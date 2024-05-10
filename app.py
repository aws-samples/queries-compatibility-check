#!/usr/bin/env python3
import os
import aws_cdk as cdk

from infrastructure.queries_compatibility_check_stack import QueriesCompatibilityCheckStack
from infrastructure import stack_input

app = cdk.App()
stack_input.init(app)
QueriesCompatibilityCheckStack(app, "QueriesCompatibilityCheckStack",
                               env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), 
                                                   region=os.getenv('CDK_DEFAULT_REGION')),)

app.synth()
