import os
from pathlib import Path

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

TEST_DATA_PATH = Path(__file__).parent / "data"
TEST_S3_BUCKET = os.environ.get("CDXT_TEST_S3_BUCKET", "commoncrawl-ci-temp")


def check_aws_s3_access():
    """Check if AWS S3 access is available."""
    try:
        s3_client = boto3.client('s3')
        # Try to list buckets as a simple check
        s3_client.list_buckets()

        s3_client.list_objects_v2(Bucket=TEST_S3_BUCKET, MaxKeys=1)

        return True
    except (NoCredentialsError, ClientError):
        return False


assert check_aws_s3_access()
