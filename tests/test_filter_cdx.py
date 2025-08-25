import pytest
from pathlib import Path

from cdx_toolkit.cli import main
from cdx_toolkit.filter_cdx import resolve_paths, validate_resolved_paths
from conftest import requires_aws_s3

fixture_path = Path(__file__).parent / "data/filter_cdx"


@requires_aws_s3
def test_cli_filter_cdx_with_surts(tmpdir, caplog):
    # check if expected number is reached
    index_path = "s3://commoncrawl/cc-index/collections"
    index_glob = "/CC-MAIN-2024-30/indexes/cdx-00187.gz"
    whitelist_path = (
        fixture_path / "whitelist_10_surts.txt"
    )  # matches on first domain and after 100k and 200k lines

    main(
        args=f"-v --limit 1140 filter_cdx {index_path} {str(whitelist_path)} {tmpdir} --filter-type surt --input-glob {index_glob}".split()
    )

    assert "Limit reached" in caplog.text


@requires_aws_s3
def test_cli_filter_cdx_with_urls(tmpdir, caplog):
    # check if expected number is reached
    index_path = "s3://commoncrawl/cc-index/collections"
    index_glob = "/CC-MAIN-2024-30/indexes/cdx-00187.gz"
    whitelist_path = (
        fixture_path / "whitelist_10_urls.txt"
    )  # matches on first domain and after 100k and 200k lines

    main(
        args=f"-v --limit 1140 filter_cdx {index_path} {str(whitelist_path)} {tmpdir} --filter-type url --input-glob {index_glob}".split()
    )

    assert "Limit reached" in caplog.text
    

@requires_aws_s3
def test_resolve_cdx_paths_from_cc_s3_to_local(tmpdir):
    tmpdir = str(tmpdir)
    base_path = "s3://commoncrawl/cc-index/collections"
    glob_pattern = "/CC-MAIN-2016-30/indexes/*.gz"

    input_files, output_files = resolve_paths(
        base_path, glob_pattern, output_base_path=tmpdir
    )

    assert len(input_files) == len(
        output_files
    ), "Input and output count must be the same"
    assert len(input_files) == 300, "Invalid input count"
    assert (
        input_files[0] == base_path + "/CC-MAIN-2016-30/indexes/cdx-00000.gz"
    ), "Invalid input file"
    assert (
        output_files[0] == tmpdir + "/CC-MAIN-2016-30/indexes/cdx-00000.gz"
    ), "Invalid output file"
    assert input_files[-1] == base_path + "/CC-MAIN-2016-30/indexes/cdx-00299.gz"


@requires_aws_s3
def test_resolve_cdx_paths_from_cc_s3_to_another_s3():
    output_base_path = "s3://some-other-bucket/filter-cdx"
    base_path = "s3://commoncrawl/cc-index/collections"
    glob_pattern = "/CC-MAIN-2016-30/indexes/cdx-000*.gz"

    input_files, output_files = resolve_paths(
        base_path, glob_pattern, output_base_path=output_base_path
    )

    assert len(input_files) == len(
        output_files
    ), "Input and output count must be the same"
    assert len(input_files) == 100, "Invalid input count"
    assert (
        input_files[0] == base_path + "/CC-MAIN-2016-30/indexes/cdx-00000.gz"
    ), "Invalid input file"
    assert (
        output_files[0] == output_base_path + "/CC-MAIN-2016-30/indexes/cdx-00000.gz"
    ), "Invalid output file"
    assert input_files[-1] == base_path + "/CC-MAIN-2016-30/indexes/cdx-00099.gz"


@requires_aws_s3
def test_filter_cdx_nonexistent_surt_file_exits(tmpdir, caplog):
    index_path = "s3://commoncrawl/cc-index/collections"
    index_glob = "/CC-MAIN-2024-30/indexes/cdx-00187.gz"
    nonexistent_surt_file = str(tmpdir / "nonexistent_surts.txt")
    
    # Test that the command exits when SURT file doesn't exist
    with pytest.raises(SystemExit) as exc_info:
        main(
            args=f"-v --limit 1140 filter_cdx {index_path} {nonexistent_surt_file} {tmpdir} --input-glob {index_glob}".split()
        )
    
    assert exc_info.value.code == 1
    assert f"Filter file not found: {nonexistent_surt_file}" in caplog.text


def test_resolve_paths_no_files_found_exits(tmpdir, caplog):
    # Test that resolve_paths exits when no files match the glob pattern
    with pytest.raises(SystemExit) as exc_info:
        resolve_paths(
            input_base_path=str(tmpdir),
            input_glob="/nonexistent-pattern-*.gz",
            output_base_path=str(tmpdir)
        )
    
    assert exc_info.value.code == 1
    assert "No files found matching glob pattern:" in caplog.text


def test_validate_resolved_paths_existing_file_exits(tmpdir, caplog):
    # Create an existing output file
    existing_file = tmpdir / "existing_output.txt"
    existing_file.write_text("existing content", encoding="utf-8")
    
    output_paths = [str(existing_file)]
    
    # Test that validate_resolved_paths exits when output file exists and overwrite=False
    with pytest.raises(SystemExit) as exc_info:
        validate_resolved_paths(output_paths, overwrite=False)
    
    assert exc_info.value.code == 1
    assert f"Output file already exists: {str(existing_file)}" in caplog.text
    assert "Use --overwrite to overwrite existing files" in caplog.text


@requires_aws_s3
def test_cli_filter_cdx_with_parallel_processing(tmpdir, caplog):
    """Test that parallel processing works correctly and processes multiple files."""
    index_path = "s3://commoncrawl/cc-index/collections"
    index_glob = "/CC-MAIN-2024-30/indexes/cdx-0018[78].gz"  # Multiple files pattern
    whitelist_path = fixture_path / "whitelist_11_surts.txt"  # Additonal entry for cdx-00188.gz

    # Run with parallel processing (2 workers)
    main(
        args=f"-v --limit 10 filter_cdx {index_path} {str(whitelist_path)} {tmpdir} --filter-type surt --input-glob {index_glob} --parallel 2".split()
    )

    # Check that multiple files were processed in parallel
    assert "Found" in caplog.text and "files matching pattern" in caplog.text
    assert "File statistics for" in caplog.text
    assert "Total statistics:" in caplog.text
    
    # Should have processed multiple files (pattern matches 2 files: cdx-00187.gz and cdx-00188.gz)
    file_stats_count = caplog.text.count("File statistics for")
    assert file_stats_count == 2, "Should process exactly 2 files with the glob pattern"

