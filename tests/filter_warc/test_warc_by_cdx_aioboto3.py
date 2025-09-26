import asyncio
from io import BytesIO

import aioboto3

from tests.conftest import requires_aws_s3, TEST_DATA_PATH

from warcio import WARCWriter
from cdx_toolkit.filter_warc.aioboto3_warc_filter import get_range_jobs_from_index_paths, write_warc, _STOP
from cdx_toolkit.filter_warc.aioboto3_utils import RangePayload, parse_s3_uri
from tests.filter_warc.test_warc_by_cdx import assert_cli_warc_by_cdx

fixture_path = TEST_DATA_PATH / 'warc_by_cdx'
aioboto3_warc_filename = 'TEST_warc_by_index-000000-001.extracted.warc.gz'  # due to parallel writer


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3_in_parallel_aioboto3(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
            '--implementation=aioboto3',
        ],
        warc_filename=aioboto3_warc_filename,
    )


def test_warc_info():
    warc_version = '1.0'
    gzip = False
    file_handler = BytesIO()
    filename = 'foo.warc'

    info = {
        'software': 'pypi_cdx_toolkit/123',
        'isPartOf': 'bar',
        'description': 'warc extraction based on CDX generated with: xx',
        'format': 'WARC file version 1.0',
    }

    writer = WARCWriter(file_handler, gzip=gzip, warc_version=warc_version)
    warcinfo = writer.create_warcinfo_record(filename, info)

    writer.write_record(warcinfo)

    file_value = file_handler.getvalue().decode('utf-8')

    assert 'pypi_cdx_toolkit/123' in file_value


@requires_aws_s3
def test_write_warc_with_file_rotation(s3_tmpdir):
    """Test write_warc function with file size rotation"""

    async def run_test():
        # Setup test data
        index_path = fixture_path / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'
        warc_download_prefix = 's3://commoncrawl'
        output_prefix_path = f'{s3_tmpdir}/file_rotation_test'

        # Use small file size to force rotation (100 KB)
        max_file_size = 100 * 1024  # 100 KB

        # Create asyncio queues
        key_queue = asyncio.Queue()
        item_queue = asyncio.Queue()

        # Writer info for WARC header
        writer_info = {
            'software': 'cdx_toolkit test',
            'operator': 'test',
            'creator': 'test',
            'description': 'Test WARC with file rotation',
        }

        # Setup S3 client
        from botocore.config import Config

        boto_cfg = Config(
            region_name='us-east-1',
            retries={'max_attempts': 3, 'mode': 'standard'},
            connect_timeout=10,
            read_timeout=120,
        )

        session = aioboto3.Session()

        async with session.client('s3', config=boto_cfg) as s3:
            # Generate range jobs from CDX file
            await get_range_jobs_from_index_paths(
                key_queue=key_queue,
                index_paths=[str(index_path)],
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

            # Create mock RangePayload objects with dummy data to simulate large content
            # Each payload will be ~30KB to force multiple file rotations
            dummy_data = b'A' * (30 * 1024)  # 30KB of dummy data

            for job in range_jobs:
                payload = RangePayload(job=job, data=dummy_data)
                await item_queue.put(payload)

            # Add stop signal
            await item_queue.put(_STOP)

            # Run write_warc function
            await write_warc(
                consumer_id=0,
                item_queue=item_queue,
                s3=s3,
                max_attempts=3,
                base_backoff_seconds=0.5,
                prefix_path=output_prefix_path,
                writer_info=writer_info,
                max_file_size=max_file_size,
                gzip=True,
            )

            # Verify that multiple WARC files were created
            dest_bucket, dest_prefix = parse_s3_uri(output_prefix_path)

            # List objects to find all created WARC files
            response = await s3.list_objects_v2(Bucket=dest_bucket, Prefix=dest_prefix)

            warc_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.extracted.warc.gz'):
                        warc_files.append(obj['Key'])

            # Assert that more than one WARC file was created
            assert len(warc_files) == 4, f'Expected multiple WARC files, but found {len(warc_files)}: {warc_files}'

            # Verify filename pattern includes sequence numbers
            for warc_file in warc_files:
                filename = warc_file.split('/')[-1]
                # Should match pattern: prefix-000000-XXX.extracted.warc.gz
                assert '-000000-' in filename, f"Filename doesn't contain expected sequence pattern: {filename}"

            # Clean up created files
            for warc_file in warc_files:
                await s3.delete_object(Bucket=dest_bucket, Key=warc_file)

    # Run the async test
    asyncio.run(run_test())
