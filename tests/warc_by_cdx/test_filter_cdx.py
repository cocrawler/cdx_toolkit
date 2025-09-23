import pytest

from unittest.mock import patch

from cdx_toolkit.cli import main
from cdx_toolkit.filter_cdx import _process_single_file, resolve_paths, validate_resolved_paths, filter_cdx
from cdx_toolkit.filter_cdx.matcher import TupleMatcher
from tests.conftest import requires_aws_s3, TEST_DATA_PATH

fixture_path = TEST_DATA_PATH / 'filter_cdx'


@requires_aws_s3
def test_cli_filter_cdx_with_surts(tmpdir, caplog):
    # check if expected number is reached
    index_path = 's3://commoncrawl/cc-index/collections'
    index_glob = '/CC-MAIN-2024-30/indexes/cdx-00187.gz'
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
            f'--input-glob={index_glob}',
        ]
    )

    assert 'Limit reached' in caplog.text


@requires_aws_s3
def test_cli_filter_cdx_with_urls(tmpdir, caplog):
    # check if expected number is reached
    index_path = 's3://commoncrawl/cc-index/collections'
    index_glob = '/CC-MAIN-2024-30/indexes/cdx-00187.gz'
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
            f'--input-glob={index_glob}',
        ]
    )

    assert 'Limit reached' in caplog.text


@requires_aws_s3
def test_resolve_cdx_paths_from_cc_s3_to_local(tmpdir):
    tmpdir = str(tmpdir)
    base_path = 's3://commoncrawl/cc-index/collections'
    glob_pattern = '/CC-MAIN-2016-30/indexes/*.gz'

    input_files, output_files = resolve_paths(base_path, glob_pattern, output_base_path=tmpdir)

    assert len(input_files) == len(output_files), 'Input and output count must be the same'
    assert len(input_files) == 300, 'Invalid input count'
    assert input_files[0] == base_path + '/CC-MAIN-2016-30/indexes/cdx-00000.gz', 'Invalid input file'
    assert output_files[0] == tmpdir + '/CC-MAIN-2016-30/indexes/cdx-00000.gz', 'Invalid output file'
    assert input_files[-1] == base_path + '/CC-MAIN-2016-30/indexes/cdx-00299.gz'


@requires_aws_s3
def test_resolve_cdx_paths_from_cc_s3_to_another_s3():
    output_base_path = 's3://some-other-bucket/filter-cdx'
    base_path = 's3://commoncrawl/cc-index/collections'
    glob_pattern = '/CC-MAIN-2016-30/indexes/cdx-000*.gz'

    input_files, output_files = resolve_paths(base_path, glob_pattern, output_base_path=output_base_path)

    assert len(input_files) == len(output_files), 'Input and output count must be the same'
    assert len(input_files) == 100, 'Invalid input count'
    assert input_files[0] == base_path + '/CC-MAIN-2016-30/indexes/cdx-00000.gz', 'Invalid input file'
    assert output_files[0] == output_base_path + '/CC-MAIN-2016-30/indexes/cdx-00000.gz', 'Invalid output file'
    assert input_files[-1] == base_path + '/CC-MAIN-2016-30/indexes/cdx-00099.gz'


@requires_aws_s3
def test_filter_cdx_nonexistent_surt_file_exits(tmpdir, caplog):
    index_path = 's3://commoncrawl/cc-index/collections'
    index_glob = '/CC-MAIN-2024-30/indexes/cdx-00187.gz'
    nonexistent_surt_file_name = 'nonexistent_surts.txt'
    nonexistent_surt_file = str(tmpdir / nonexistent_surt_file_name)

    # Test that the command exits when SURT file doesn't exist
    with pytest.raises(SystemExit) as exc_info:
        main(
            args=[
                '-v',
                '--limit=1140',
                'filter_cdx',
                f'{index_path}',
                f'{nonexistent_surt_file}',
                f'{tmpdir}',
                f'--input-glob={index_glob}',
            ]
        )

    assert exc_info.value.code == 1
    assert 'Filter file not found: ' in caplog.text
    assert nonexistent_surt_file_name in caplog.text


def test_resolve_paths_no_files_found_exits(tmpdir, caplog):
    # Test that resolve_paths exits when no files match the glob pattern
    with pytest.raises(SystemExit) as exc_info:
        resolve_paths(input_base_path=str(tmpdir), input_glob='/nonexistent-pattern-*.gz', output_base_path=str(tmpdir))

    assert exc_info.value.code == 1
    assert 'No files found matching glob pattern:' in caplog.text


def test_validate_resolved_paths_existing_file_exits(tmpdir, caplog):
    # Create an existing output file
    existing_file = tmpdir / 'existing_output.txt'
    existing_file.write_text('existing content', encoding='utf-8')

    output_paths = [str(existing_file)]

    # Test that validate_resolved_paths exits when output file exists and overwrite=False
    with pytest.raises(SystemExit) as exc_info:
        validate_resolved_paths(output_paths, overwrite=False)

    assert exc_info.value.code == 1
    assert f'Output file already exists: {str(existing_file)}' in caplog.text
    assert 'Use --overwrite to overwrite existing files' in caplog.text


@requires_aws_s3
def test_cli_filter_cdx_with_parallel_processing(tmpdir, caplog):
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


def test_process_single_file(tmpdir):
    input_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    matcher = TupleMatcher(prefixes=['fr,'])

    lines_n, included_n = _process_single_file(
        input_path=input_path,
        output_path=tmpdir + '/filter_cdx',
        matcher=matcher,
        log_every_n=10,
        limit=100,
    )

    assert included_n == 100
    assert lines_n == 100


def test_process_single_file_empty(tmpdir):
    input_path = tmpdir + '/input'
    with open(input_path, 'w') as f:
        f.write('')

    lines_n, included_n = _process_single_file(
        input_path=input_path,
        output_path=tmpdir + '/output',
        matcher=None,
    )
    assert lines_n == 0
    assert included_n == 0


def test_filter_cdx_error_handling(tmpdir, caplog):
    """Test filter_cdx function error handling when exceptions occur during processing."""

    def mock_process_single_file(*args, **kwargs):
        raise ValueError()

    # Create test input and output paths
    input_paths = [str(tmpdir / 'input1.cdx'), str(tmpdir / 'input2.cdx')]
    output_paths = [str(tmpdir / 'output1.cdx'), str(tmpdir / 'output2.cdx')]

    # Replace the _process_single_file function with our mock
    with patch('cdx_toolkit.filter_cdx._process_single_file', side_effect=mock_process_single_file):
        # Test the error handling
        total_lines, total_included, total_errors = filter_cdx(
            matcher=None,
            input_paths=input_paths,
            output_paths=output_paths,
        )

        # Verify error handling results
        assert total_errors == 2, f'Should have 1 error from the failed file, got {total_errors}'
        assert total_lines == 0, 'Should have lines from the successful file'
        assert total_included == 0, 'Should have included lines from the successful file'

        # Check that error was logged correctly
        assert 'generated an exception' in caplog.text
