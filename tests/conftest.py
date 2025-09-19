import os
from pathlib import Path
import pytest
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

TEST_DATA_PATH = Path(__file__).parent / "data"
TEST_S3_BUCKET = os.environ.get("CDXT_TEST_S3_BUCKET", "commoncrawl-ci-temp")


def check_aws_s3_access():
    """Check if AWS S3 access is available."""
    try:
        s3_client = boto3.client('s3')

        # Try list objects on test bucket
        s3_client.list_objects_v2(Bucket=TEST_S3_BUCKET, MaxKeys=1)
        return True
    except (NoCredentialsError, ClientError):
        return False


def requires_aws_s3(func):
    """Pytest decorator that skips test if AWS S3 access is not available."""
    return pytest.mark.skipif(
        not check_aws_s3_access(),
        reason="AWS S3 access not available (no credentials or permissions)"
    )(func)