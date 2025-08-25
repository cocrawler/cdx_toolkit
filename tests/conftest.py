import pytest
import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def check_aws_s3_access():
    """Check if AWS S3 access is available."""
    try:
        s3_client = boto3.client('s3')
        # Try to list buckets as a simple check
        s3_client.list_buckets()
        return True
    except (NoCredentialsError, ClientError):
        return False


def requires_aws_s3(func):
    """Pytest decorator that skips test if AWS S3 access is not available."""
    return pytest.mark.skipif(
        not check_aws_s3_access(),
        reason="AWS S3 access not available (no credentials or permissions)"
    )(func)