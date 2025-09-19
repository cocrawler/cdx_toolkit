import os
from pathlib import Path

import boto3

TEST_DATA_PATH = Path(__file__).parent / "data"
TEST_S3_BUCKET = os.environ.get("CDXT_TEST_S3_BUCKET", "commoncrawl-ci-temp")


def check_aws_s3_access():
    """Check if AWS S3 access is available."""

    session = boto3.Session()
    credentials = session.get_credentials()
    if credentials:
        print(f'Access Key: {credentials.access_key[:10]}...')
        print(f'Secret Key: {"SET" if credentials.secret_key else "NOT SET"}')
        print(f'Token: {"SET" if credentials.token else "NOT SET"}')
    else:
        print('No credentials found by boto3')
        
    s3_client = boto3.client('s3')
    # Try to list buckets as a simple check
    s3_client.list_buckets()

    s3_client.list_objects_v2(Bucket=TEST_S3_BUCKET, MaxKeys=1)

    return True


assert check_aws_s3_access()
