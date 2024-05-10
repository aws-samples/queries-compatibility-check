from constructs import Construct
from aws_cdk import aws_ec2 as ec2

class SecurityGroup(Construct):
    def __init__(self, scope: Construct, id: str, vpc: ec2.Vpc, env_name:str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create a security group allowing all UDP traffic within the VPC
        self.udp_security_group = ec2.SecurityGroup(
            self,
            'UDPSecurityGroup-{}'.format(env_name),
            security_group_name='UDPSecurityGroup-{}'.format(env_name),
            vpc=vpc,
            description='Allow UDP traffic on port 4789 within the VPC',
            allow_all_outbound=True,
        )

        # Add an inbound rule to allow all UDP traffic from within the VPC
        self.udp_security_group.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block),
            ec2.Port.udp(4789),
            'Allow UDP traffic on port 4789 from within the VPC',
        )

    @property
    def security_group(self):
        return self.udp_security_group