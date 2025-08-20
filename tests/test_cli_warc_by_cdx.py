from pathlib import Path
from cdx_toolkit.cli import main
from cdx_toolkit.warcer_by_cdx import (
    get_caputure_objects_from_index,
    get_index_from_path,
)

fixture_path = Path(__file__).parent / "data/warc_by_cdx"


def test_warc_by_cdx(tmpdir, caplog):
    # test cli and check log output
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    main(
        args=f"""-v --cc --cc-mirror https://index.commoncrawl.org/ --limit 10  warc_by_cdx {str(index_path)} --prefix {str(tmpdir)}/TEST_warc_by_index --creator creator --operator bob""".split()
    )

    assert "Limit reached" in caplog.text


def test_get_caputure_objects_from_index():
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    for obj in get_caputure_objects_from_index(get_index_from_path(index_path)):
        break

    assert obj.data["length"] == "9754"
