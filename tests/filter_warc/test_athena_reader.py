import asyncio
from cdx_toolkit.filter_warc.aioboto3_warc_filter import _STOP
from cdx_toolkit.filter_warc.athena_reader import get_databases, get_range_jobs_from_athena
from tests.conftest import requires_aws_athena

import boto3


@requires_aws_athena
def test_get_databases():
    from botocore.config import Config
    import boto3

    boto_cfg = Config(
        region_name='us-east-1',
    )
    athena_client = boto3.client('athena', config=boto_cfg)
    dbs = get_databases(client=athena_client)
    assert 'ccindex' in dbs


@requires_aws_athena
def test_get_range_jobs_from_athena():
    async def run_test():
        # Setup test data
        warc_download_prefix = 's3://commoncrawl'

        # Create asyncio queues
        key_queue = asyncio.Queue()

        # Setup S3 client
        from botocore.config import Config

        boto_cfg = Config(
            region_name='us-east-1',
            retries={'max_attempts': 3, 'mode': 'standard'},
            connect_timeout=10,
            read_timeout=120,
        )

        athena_client = boto3.client('athena', config=boto_cfg)

        # Generate range jobs from Athena query
        await get_range_jobs_from_athena(
            client=athena_client,
            database="ccindex",
            s3_output_location="s3://commoncrawl-ci-temp/athena-results/",
            url_host_names=[
                'oceancolor.sci.gsfc.nasa.gov',
            ],
            key_queue=key_queue,
            warc_download_prefix=warc_download_prefix,
            num_fetchers=1,
            limit=10,  # Use 10 records to ensure we have enough data
        )

        # Collect all range jobs
        range_jobs = []
        while not key_queue.empty():
            job = await key_queue.get()
            if job is not _STOP:
                range_jobs.append(job)
            key_queue.task_done()

        assert len(range_jobs) == 10, "Invalid range jobs count"

    # Run the async test
    asyncio.run(run_test())
