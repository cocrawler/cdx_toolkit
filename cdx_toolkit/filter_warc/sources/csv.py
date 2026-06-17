import csv
import logging
from typing import Iterator, Optional

import fsspec

from cdx_toolkit.filter_warc.data_classes import RangeJob
from cdx_toolkit.filter_warc.sources.base import RangeJobSource
from cdx_toolkit.filter_warc.sources.sql_base import join_warc_url


logger = logging.getLogger(__name__)

# Fetch-job columns name the WARC *location* (mirroring the columnar index's column
# names) so they never collide with extra analysis columns a raw --query may SELECT
# -- e.g. the index's own `url` (the captured page URL) vs `warc_url` (download URL).
FILENAME_FIELDS = ['warc_filename', 'warc_record_offset', 'warc_record_length']
URL_FIELDS = ['warc_url', 'warc_record_offset', 'warc_record_length']

# Columns the reader interprets as the fetch job; everything else round-trips as extra.
_KNOWN_FIELDS = {'warc_url', 'warc_filename', 'warc_record_offset', 'warc_record_length'}


class RangeJobCsvWriter:
    """Write RangeJobs to a CSV (local or remote via fsspec).

    Default mode writes the relative `warc_filename` column (the consumer prepends
    the WARC download prefix); `self_contained` mode writes the full `warc_url`
    column. Any extra columns carried on a RangeJob (from a raw SQL --query) are
    appended after the fetch-job columns. The header is written lazily from the
    first job so its extra columns can be discovered."""

    def __init__(self, path: str, self_contained: bool = False):
        self.path = path
        self.self_contained = self_contained
        self._base_fields = URL_FIELDS if self_contained else FILENAME_FIELDS
        self._ctx = fsspec.open(path, 'wt', newline='')
        self._fh = self._ctx.__enter__()
        self._writer = None

    def _init_writer(self, fieldnames) -> None:
        self._writer = csv.DictWriter(
            self._fh, fieldnames=fieldnames, extrasaction='ignore', restval=''
        )
        self._writer.writeheader()

    def write(self, job: RangeJob) -> None:
        if self.self_contained:
            row = {
                'warc_url': job.url,
                'warc_record_offset': job.offset,
                'warc_record_length': job.length,
            }
        else:
            if job.filename is None:
                raise ValueError(
                    'cannot write a non-self-contained range-jobs CSV: RangeJob.filename is '
                    'missing; pass --csv-self-contained to write full URLs instead'
                )
            row = {
                'warc_filename': job.filename,
                'warc_record_offset': job.offset,
                'warc_record_length': job.length,
            }
        if job.extra:
            row.update(job.extra)

        if self._writer is None:
            extra_fields = [k for k in (job.extra or {}) if k not in self._base_fields]
            self._init_writer(self._base_fields + extra_fields)
        self._writer.writerow(row)

    def close(self) -> None:
        if self._ctx is not None:
            # No rows written: still emit the base header for a valid (empty) file.
            if self._writer is None:
                self._init_writer(self._base_fields)
            self._ctx.__exit__(None, None, None)
            self._ctx = None
            self._fh = None


class CsvSource(RangeJobSource):
    """RangeJobs read from a CSV/TSV.

    Mode is auto-detected from the header: a `warc_url` column => self-contained
    (used as-is); a `warc_filename` column => the WARC download prefix is prepended.
    If both are present, a warning is logged and `warc_url` is used. Any other
    columns round-trip onto RangeJob.extra. TSV is detected from a `.tsv`/`.tsv.gz`
    extension; `.gz` inputs are decompressed."""

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
            has_url = 'warc_url' in fields
            has_filename = 'warc_filename' in fields
            if has_url and has_filename:
                logger.warning(
                    'range-jobs CSV %s has both `warc_url` and `warc_filename` columns; '
                    'using `warc_url` for fetching', self.path
                )
                mode_url = True
            elif has_url:
                mode_url = True
            elif has_filename:
                mode_url = False
            else:
                raise ValueError(
                    f'range-jobs CSV {self.path} must have a `warc_url` or `warc_filename` column '
                    f'(got header: {reader.fieldnames})'
                )

            for row in reader:
                offset = int(row['warc_record_offset'])
                length = int(row['warc_record_length'])
                if mode_url:
                    url = row['warc_url']
                    filename = row.get('warc_filename')
                else:
                    filename = row['warc_filename']
                    url = join_warc_url(self.warc_download_prefix, filename)
                extra = {k: v for k, v in row.items() if k not in _KNOWN_FIELDS}
                yield RangeJob(
                    url=url, offset=offset, length=length,
                    filename=filename, extra=extra or None,
                )
