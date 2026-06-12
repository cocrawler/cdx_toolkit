import csv
import logging
from typing import Iterator, Optional

import fsspec

from cdx_toolkit.filter_warc.data_classes import RangeJob
from cdx_toolkit.filter_warc.sources.base import RangeJobSource
from cdx_toolkit.filter_warc.sources.sql_base import join_warc_url


logger = logging.getLogger(__name__)

FILENAME_FIELDS = ['filename', 'offset', 'length']
URL_FIELDS = ['url', 'offset', 'length']


class RangeJobCsvWriter:
    """Write RangeJobs to a CSV (local or remote via fsspec).

    Default mode writes the relative `filename` column (the consumer prepends the
    WARC download prefix); `self_contained` mode writes the full `url` column."""

    def __init__(self, path: str, self_contained: bool = False):
        self.path = path
        self.self_contained = self_contained
        self._fields = URL_FIELDS if self_contained else FILENAME_FIELDS
        self._ctx = fsspec.open(path, 'wt', newline='')
        self._fh = self._ctx.__enter__()
        self._writer = csv.DictWriter(self._fh, fieldnames=self._fields)
        self._writer.writeheader()

    def write(self, job: RangeJob) -> None:
        if self.self_contained:
            row = {'url': job.url, 'offset': job.offset, 'length': job.length}
        else:
            if job.filename is None:
                raise ValueError(
                    'cannot write a non-self-contained range-jobs CSV: RangeJob.filename is '
                    'missing; pass --csv-self-contained to write full URLs instead'
                )
            row = {'filename': job.filename, 'offset': job.offset, 'length': job.length}
        self._writer.writerow(row)

    def close(self) -> None:
        if self._ctx is not None:
            self._ctx.__exit__(None, None, None)
            self._ctx = None
            self._fh = None


class CsvSource(RangeJobSource):
    """RangeJobs read from a CSV/TSV.

    Mode is auto-detected from the header: a `url` column => self-contained (used
    as-is); a `filename` column => the WARC download prefix is prepended. TSV is
    detected from a `.tsv`/`.tsv.gz` extension; `.gz` inputs are decompressed."""

    def __init__(self, path: str, warc_download_prefix: Optional[str]):
        self.path = path
        self.warc_download_prefix = warc_download_prefix

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        path = str(self.path)
        delimiter = '\t' if path.endswith(('.tsv', '.tsv.gz')) else ','
        compression = 'gzip' if path.endswith('.gz') else None

        with fsspec.open(self.path, 'rt', newline='', compression=compression) as fh:
            reader = csv.DictReader(fh, delimiter=delimiter)
            fields = set(reader.fieldnames or [])
            if 'url' in fields:
                mode_url = True
            elif 'filename' in fields:
                mode_url = False
            else:
                raise ValueError(
                    f'range-jobs CSV {self.path} must have a `url` or `filename` column '
                    f'(got header: {reader.fieldnames})'
                )

            for row in reader:
                offset = int(row['offset'])
                length = int(row['length'])
                if mode_url:
                    url = row['url']
                    filename = None
                else:
                    filename = row['filename']
                    url = join_warc_url(self.warc_download_prefix, filename)
                yield RangeJob(url=url, offset=offset, length=length, filename=filename)
