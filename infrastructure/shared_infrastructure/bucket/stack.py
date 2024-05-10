from aws_cdk import (
    aws_s3,
    aws_s3_deployment as s3deploy
    )
from constructs import Construct


class Bucket(Construct):
    def __init__(self, scope: Construct, construct_id: str, account: str, region: str, env_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # provision a s3 bucket
        self.bucket = aws_s3.Bucket(
            self, "db-check-bucket",
            bucket_name="db-check-bucket-{}-{}-{}".format(account, region, env_name),
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL
            )

        # Upload agent code and file to S3 bucket
        s3deploy.BucketDeployment(self, "DeployFiles",
                                  sources=[s3deploy.Source.asset("agent/")],
                                  destination_bucket=self.bucket,
                                  destination_key_prefix="code"
                                  )