import logging
from typing import Iterator, List

from cdx_toolkit.filter_warc.cdx_utils import iter_cdx_index_from_path
from cdx_toolkit.filter_warc.data_classes import RangeJob
from cdx_toolkit.filter_warc.sources.base import RangeJobSource


logger = logging.getLogger(__name__)


class CdxSource(RangeJobSource):
    """RangeJobs read from one or more CDX index files (local or remote via fsspec)."""

    def __init__(self, cdx_paths: List[str], warc_download_prefix: str):
        self.cdx_paths = cdx_paths
        self.warc_download_prefix = warc_download_prefix

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        for index_path in self.cdx_paths:
            try:
                for warc_url, offset, length, filename in iter_cdx_index_from_path(
                    index_path, self.warc_download_prefix
                ):
                    yield RangeJob(url=warc_url, offset=offset, length=length, filename=filename)
            except Exception as e:
                # Preserve the previous behaviour of skipping a bad index file.
                logger.error('Failed to read CDX index from %s: %s', index_path, e)
