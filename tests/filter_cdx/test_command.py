import pytest


from cdx_toolkit.cli import main
from tests.conftest import TEST_DATA_PATH, requires_aws_s3

fixture_path = TEST_DATA_PATH / 'filter_cdx'


@requires_aws_s3
def test_cli_filter_cdx_from_s3_with_parallel_processing(tmpdir, caplog):
    """Test that parallel processing works correctly and processes multiple files."""
    index_path = 's3://commoncrawl/cc-index/collections'
    index_glob = '/CC-MAIN-2024-30/indexes/cdx-0018[78].gz'  # Multiple files pattern
    whitelist_path = fixture_path / 'whitelist_11_surts.txt'  # Additonal entry for cdx-00188.gz

    # Run with parallel processing (2 workers)
    main(
        args=[
            '-v',
            '--limit=10',
            'filter_cdx',
            f'{index_path}',
            f'{str(whitelist_path)}',
            f'{tmpdir}',
            '--filter-type=surt',
            f'--input-glob={index_glob}',
            '--parallel=2',
        ]
    )

    # Check that multiple files were processed in parallel
    assert 'Found' in caplog.text and 'files matching pattern' in caplog.text
    assert 'File statistics' in caplog.text
    assert 'Filter statistics' in caplog.text

    # Should have processed multiple files (pattern matches 2 files: cdx-00187.gz and cdx-00188.gz)
    file_stats_count = caplog.text.count('File statistics')
    assert file_stats_count == 2, 'Should process exactly 2 files with the glob pattern'


def test_filter_cdx_nonexistent_surt_file_exits(tmpdir, caplog):
    index_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    nonexistent_surt_file_name = 'nonexistent_surts.txt'
    nonexistent_surt_file = str(tmpdir / nonexistent_surt_file_name)

    # Test that the command exits when SURT file doesn't exist
    with pytest.raises(SystemExit) as exc_info:
        main(
            args=[
                '-v',
                '--limit=1140',
                'filter_cdx',
                f'{str(index_path)}',
                f'{nonexistent_surt_file}',
                f'{tmpdir}',
                '--overwrite',
            ]
        )

    assert exc_info.value.code == 1
    assert 'Filter file not found: ' in caplog.text
    assert nonexistent_surt_file_name in caplog.text


def test_cli_filter_cdx_with_wildcard_urls(tmpdir, caplog):
    # check if expected number is reached
    index_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    whitelist_path = fixture_path / 'whitelist_wildcard_urls.txt'  # matches on all .com and .fr host names

    main(
        args=[
            '-v',
            '--limit=10',
            'filter_cdx',
            f'{index_path}',
            f'{str(whitelist_path)}',
            f'{tmpdir}',
            '--filter-type=url',
            '--overwrite',
        ]
    )

    assert 'Limit reached' in caplog.text


def test_cli_filter_cdx_with_urls(tmpdir, caplog):
    # check if expected number is reached
    index_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    whitelist_path = fixture_path / 'whitelist_10_urls.txt'  # matches on first domain and after 100k and 200k lines

    main(
        args=[
            '-v',
            '--limit=1140',
            'filter_cdx',
            f'{index_path}',
            f'{str(whitelist_path)}',
            f'{tmpdir}',
            '--filter-type=url',
            '--overwrite',
        ]
    )

    assert 'Limit reached' in caplog.text


def test_cli_filter_cdx_with_surts(tmpdir, caplog):
    # check if expected number is reached
    index_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    whitelist_path = fixture_path / 'whitelist_10_surts.txt'  # matches on first domain and after 100k and 200k lines

    main(
        args=[
            '-v',
            '--limit=1140',
            'filter_cdx',
            f'{index_path}',
            f'{str(whitelist_path)}',
            f'{tmpdir}',
            '--filter-type=surt',
            '--overwrite',
        ]
    )

    assert 'Limit reached' in caplog.text
