from pathlib import Path

from cdx_toolkit.cli import main
from cdx_toolkit.filter_cdx import resolve_paths

fixture_path = Path(__file__).parent / "data/filter_cdx"


def test_filter_cdx(tmpdir, caplog):
    # check if expected number is reached
    index_path = "s3://commoncrawl/cc-index/collections"
    index_glob = "/CC-MAIN-2024-30/indexes/cdx-00187.gz"
    whitelist_path = (
        fixture_path / "whitelist_10_surts.txt"
    )  # matches on first domain and after 100k and 200k lines

    main(
        args=f"-v --limit 1140 filter_cdx {index_path} {str(whitelist_path)} {tmpdir} --input_glob {index_glob}".split()
    )

    assert "Limit reached" in caplog.text


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


if __name__ == "__main__":
    test_resolve_cdx_paths_from_cc_s3_to_local("./data/tmp")
