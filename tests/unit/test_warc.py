import cdx_toolkit.warc
from tests.conftest import requires_aws_s3


def test_wb_redir_to_original():
    location = 'https://web.archive.org/web/20110209062054id_/http://commoncrawl.org/'
    ret = 'http://commoncrawl.org/'
    assert cdx_toolkit.warc.wb_redir_to_original(location) == ret


@requires_aws_s3
def test_fetch_warc_record_from_s3():
    record = cdx_toolkit.warc.fetch_warc_record(
        capture={
            'url': 'https://bibliotheque.missiondefrance.fr/index.php?lvl=bulletin_display&id=319',
            'filename': 'crawl-data/CC-MAIN-2024-30/segments/1720763514759.37/warc/CC-MAIN-20240716142214-20240716172214-00337.warc.gz',  # noqa: E501
            'offset': 111440525,
            'length': 9754,
        },
        warc_download_prefix='s3://commoncrawl',
    )
    record_content = record.content_stream().read().decode(errors='ignore')

    assert record.rec_type == 'response'
    assert record.length == 75825
    assert '<title>Catalogue en ligne Mission de France</title>' in record_content
