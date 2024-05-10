from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_elasticloadbalancingv2 as elbv2,
)
from constructs import Construct

class TrafficMirroring(Construct):
    def __init__(self, scope: Construct, id: str, vpc: ec2.Vpc, env_name:str, nlb: elbv2.NetworkLoadBalancer, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create a Traffic Mirror Target
        self.traffic_mirror_target = ec2.CfnTrafficMirrorTarget(
            self, "TrafficMirrorTarget",
            network_load_balancer_arn=nlb.load_balancer_arn,
            description="Traffic Mirror Target for NLB - {}".format(env_name),
        )

        # Create a Traffic Mirror Filter
        self.traffic_mirror_filter = ec2.CfnTrafficMirrorFilter(
            self, "TrafficMirrorFilter",
            description="Traffic Mirror Filter - {}".format(env_name),
            network_services=[
                "amazon-dns"
            ]
        )

        # Create a Traffic Mirror Filter rule allows traffic within the VPC
        traffic_mirror_filter_rule = ec2.CfnTrafficMirrorFilterRule(self, "TrafficMirrorFilterRule",
            destination_cidr_block=vpc.vpc_cidr_block,
            rule_action="accept",
            rule_number=100,
            source_cidr_block=vpc.vpc_cidr_block,
            traffic_direction="ingress",
            traffic_mirror_filter_id=self.traffic_mirror_filter.ref,

            # the properties below are optional
            description="Traffic Mirror Filter rule allows traffic within the VPC",
            protocol=6,
        )

    @property
    def tmt(self):
        return self.traffic_mirror_target

    @property
    def tmf(self):
        return self.traffic_mirror_filter 