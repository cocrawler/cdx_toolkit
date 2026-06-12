import csv

import pytest

from cdx_toolkit.filter_warc.data_classes import RangeJob
from cdx_toolkit.filter_warc.sources.csv import RangeJobCsvWriter, CsvSource


def test_writer_filename_mode_and_read(tmp_path):
    path = str(tmp_path / 'ranges.csv')
    w = RangeJobCsvWriter(path, self_contained=False)
    w.write(RangeJob(
        url='https://data.commoncrawl.org/crawl-data/x.warc.gz',
        offset=10, length=20, filename='crawl-data/x.warc.gz',
    ))
    w.close()

    with open(path, newline='') as f:
        rows = list(csv.DictReader(f))
    assert rows[0] == {'filename': 'crawl-data/x.warc.gz', 'offset': '10', 'length': '20'}

    jobs = list(CsvSource(path, 'https://data.commoncrawl.org').iter_range_jobs())
    assert jobs[0].url == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    assert jobs[0].offset == 10 and jobs[0].length == 20
    assert jobs[0].filename == 'crawl-data/x.warc.gz'


def test_writer_self_contained_mode_and_read(tmp_path):
    path = str(tmp_path / 'ranges_url.csv')
    w = RangeJobCsvWriter(path, self_contained=True)
    w.write(RangeJob(url='s3://commoncrawl/crawl-data/x.warc.gz', offset=5, length=7, filename='crawl-data/x.warc.gz'))
    w.close()

    with open(path, newline='') as f:
        rows = list(csv.DictReader(f))
    assert rows[0] == {'url': 's3://commoncrawl/crawl-data/x.warc.gz', 'offset': '5', 'length': '7'}

    # url used as-is regardless of the (ignored) prefix
    jobs = list(CsvSource(path, 'https://ignored').iter_range_jobs())
    assert jobs[0].url == 's3://commoncrawl/crawl-data/x.warc.gz'
    assert jobs[0].filename is None


def test_writer_filename_mode_requires_filename(tmp_path):
    w = RangeJobCsvWriter(str(tmp_path / 'r.csv'), self_contained=False)
    with pytest.raises(ValueError):
        w.write(RangeJob(url='https://x/y.warc.gz', offset=1, length=2, filename=None))
    w.close()


def test_csv_source_missing_columns(tmp_path):
    path = str(tmp_path / 'bad.csv')
    with open(path, 'w') as f:
        f.write('foo,bar\n1,2\n')
    with pytest.raises(ValueError):
        list(CsvSource(path, 'https://x').iter_range_jobs())


def test_csv_source_tsv(tmp_path):
    path = str(tmp_path / 'ranges.tsv')
    with open(path, 'w') as f:
        f.write('filename\toffset\tlength\n')
        f.write('crawl-data/x.warc.gz\t10\t20\n')
    jobs = list(CsvSource(path, 'https://data.commoncrawl.org').iter_range_jobs())
    assert jobs[0].url == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    assert jobs[0].offset == 10 and jobs[0].length == 20
