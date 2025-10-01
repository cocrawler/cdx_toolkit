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
def test_cli_warc_by_cdx_over_s3_to_s3_in_parallel_warc_filter(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
            '--implementation=warc_filter',
        ],
        warc_filename=aioboto3_warc_filename,
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_http_to_s3_in_parallel_warc_filter(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        'https://data.commoncrawl.org',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
            '--implementation=warc_filter',
        ],
        warc_filename=aioboto3_warc_filename,
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_local_in_parallel_warc_filter(tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
            '--implementation=warc_filter',
        ],
        warc_filename=aioboto3_warc_filename,
    )

