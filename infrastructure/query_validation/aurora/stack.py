try:
    from aws_cdk import core as cdk
except ImportError:
    import aws_cdk as cdk

from aws_cdk import (
    aws_rds as rds,
    aws_ec2 as ec2
)
from constructs import Construct


class Aurora(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name:  str, vpc: ec2.Vpc, private_subnets,
                 sg: ec2.SecurityGroup, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        cluster = rds.DatabaseCluster(self, "validation_db",
                                      cluster_identifier=f"validation-db-{env_name}",
                                      engine=rds.DatabaseClusterEngine.aurora_mysql(
                                          version=rds.AuroraMysqlEngineVersion.VER_3_04_1),
                                      credentials=rds.Credentials.from_generated_secret("admin"),
                                      writer=rds.ClusterInstance.provisioned("writer",
                                                                             instance_identifier=f"validation-writer-{env_name}",
                                                                             publicly_accessible=False,
                                                                             instance_type=ec2.InstanceType.of(
                                                                                 ec2.InstanceClass.R5,
                                                                                 ec2.InstanceSize.LARGE)
                                                                             ),
                                      readers=[
                                          rds.ClusterInstance.provisioned("reader",
                                                                          instance_identifier=f"validation-reader-{env_name}",
                                                                          instance_type=ec2.InstanceType.of(
                                                                              ec2.InstanceClass.R5,
                                                                              ec2.InstanceSize.LARGE),
                                                                          ),
                                      ],
                                      vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
                                      vpc=vpc,
                                      security_groups=[sg],
                                      storage_encrypted=True,
                                      )

        proxy = rds.DatabaseProxy(self, "validation_db_proxy",
                                  db_proxy_name=f"validation-db-proxy-{env_name}",
                                  proxy_target=rds.ProxyTarget.from_cluster(cluster),
                                  secrets=[cluster.secret],
                                  vpc=vpc,
                                  vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
                                  security_groups=[sg],
                                  client_password_auth_type=rds.ClientPasswordAuthType.MYSQL_NATIVE_PASSWORD,
                                  iam_auth=True,
                                  )

        # Set property
        self.db_proxy = proxy

    @property
    def proxy(self):
        return self.db_proxy

