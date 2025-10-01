
from tests.conftest import requires_aws_s3, TEST_DATA_PATH

from tests.filter_warc.test_warc_by_cdx import assert_cli_warc_by_cdx

fixture_path = TEST_DATA_PATH / 'warc_by_cdx'


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3_in_parallel_warc_filter(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
        ],
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_http_to_s3_in_parallel_warc_filter(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        'https://data.commoncrawl.org',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
        ],
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_local_in_parallel_warc_filter(tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
        ],
    )
