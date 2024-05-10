from constructs import Construct

from aws_cdk import (
    aws_ec2 as ec2,
    aws_elasticloadbalancingv2 as elbv2,
    aws_ec2 as ec2,
    aws_autoscaling,
)

class NLB(Construct):
    def __init__(self, scope: Construct, id: str, vpc: ec2.Vpc, private_subnets, 
                 sg: ec2.SecurityGroup, asg: aws_autoscaling.AutoScalingGroup, env_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Create the internal network load balancer
        self.target_nlb = elbv2.NetworkLoadBalancer(
            self, 
            "NLB-{}".format(env_name), 
            load_balancer_name='db-check-nlb-{}'.format(env_name),
            vpc=vpc, 
            security_groups = [sg],
            internet_facing=False, 
            cross_zone_enabled=True,
            vpc_subnets=ec2.SubnetSelection(subnets=private_subnets)
        )

        # Create a UDP listener for the NLB on port 4789
        listener = self.target_nlb.add_listener("UDPListener",
            port=4789,
            protocol=elbv2.Protocol.UDP
        )

        # Create a target group for the NLB
        target_group = listener.add_targets('TargetGroup',
            target_group_name='db-check-target-group-{}'.format(env_name),
            port=4789,
            protocol=elbv2.Protocol.UDP,
            targets=[asg]
        )

    @property
    def network_load_balancer(self):
        return self.target_nlb 