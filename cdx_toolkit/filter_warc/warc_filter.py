import asyncio
from io import BytesIO
import logging
from typing import List, Optional, Dict

import aioboto3
from botocore.config import Config
from warcio import WARCWriter

from cdx_toolkit.filter_warc.aioboto3_utils import (
    RangeJob,
    RangePayload,
    ThroughputTracker,
    is_s3_url,
    parse_s3_uri,
    ranged_get_bytes,
)
from cdx_toolkit.filter_warc.aioboto3_warc_filter import create_new_writer_with_header
from cdx_toolkit.filter_warc.aioboto3_writer import S3ShardWriter
from cdx_toolkit.filter_warc.cdx_utils import (
    iter_cdx_index_from_path,
)
from cdx_toolkit.filter_warc.warc_utils import get_bytes_from_warc_record, get_resource_record_from_path


_STOP = object()

logger = logging.getLogger(__name__)


class WARCFilter:
    """Filter WARC files using a three stage listner-producer-consumer pattern.

    Filter targets:
    - CDX index files from local or remote file system.

    WARC reader:
    - HTTP range reads
    - S3 range reads

    WARC writer:
    - Local file system
    - S3 using multi-part uploads
    """
    def __init__(
        self,
        index_paths: List[str],
        prefix_path: str,
        writer_info: Dict,
        writer_subprefix: Optional[str] = None,
        write_paths_as_resource_records: Optional[List[str]] = None,
        write_paths_as_resource_records_metadata: Optional[List[str]] = None,
        record_limit: int = 0,
        log_every_n: int = 1000,
        warc_download_prefix: Optional[str] = None,
        n_parallel: int = 1,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
        writer_kwargs: Optional[Dict] = None,
        range_jobs_queue_size: int = 1000,
        warc_records_queue_size: int = 200,
        fetcher_to_consumer_ratio: int = 6,
        aws_region_name: str = 'us-east-1',
        warc_version: str = '1.0',
        content_type: Optional[str] = None,
        min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
        max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
    ):
        self.index_paths = index_paths
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.write_paths_as_resource_records = write_paths_as_resource_records
        self.write_paths_as_resource_records_metadata = write_paths_as_resource_records_metadata
        self.record_limit = record_limit
        self.log_every_n = log_every_n
        self.warc_download_prefix = warc_download_prefix
        self.n_parallel = n_parallel
        self.writer_kwargs = writer_kwargs
        self.range_jobs_queue_size = range_jobs_queue_size
        self.warc_records_queue_size = warc_records_queue_size
        self.aws_region_name = aws_region_name
        self.fetcher_to_consumer_ratio = fetcher_to_consumer_ratio
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self.num_fetchers = n_parallel
        self.num_consumers = max(int(self.num_fetchers / self.fetcher_to_consumer_ratio), 1)
        self.gzip = self.index_paths[0].endswith('.gz') if self.index_paths else False
        self.warc_version = warc_version
        self.content_type = content_type
        self.min_part_size = min_part_size
        self.max_file_size = max_file_size

    def filter(self) -> int:
        """Perform the filtering process (calls async method via asyncio.run)."""
        try:
            return asyncio.run(self.filter_async())
        except KeyboardInterrupt:
            logger.warning('Interrupted by user.')

        return -1

    def needs_s3(self) -> bool:
        """Returns true if S3 is needed at any stage."""
        return (
            (self.index_paths is not None and len(self.index_paths) > 0 and is_s3_url(self.index_paths[0]))  # stage 1
            or is_s3_url(self.warc_download_prefix)  # stage 3
            or is_s3_url(self.prefix_path)  # stage 3
        )

    def get_s3_client(self):
        """Return s3 client if needed."""
        if self.needs_s3():
            session = aioboto3.Session()

            return session.client('s3', config=self.get_boto3_config())
        else:
            return None
        
    async def filter_async(self) -> int:
        """Filter process using a three stage approach (job generator, warc reader, warc writer)."""
        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        async with self.get_s3_client() as s3_client:
            # Fetch file paths and ranges (offset, length) from index files
            logger.info('Starting lister, %d fetchers, %d consumers', self.num_fetchers, self.num_consumers)

            job_generators = asyncio.create_task(
                self.generate_range_jobs(
                    range_jobs_queue,
                    s3_client=s3_client,
                )
            )

            # Read WARC records based on file paths and ranges
            warc_readers = [
                asyncio.create_task(
                    self.read_warc_records(
                        fetcher_id=i,
                        range_jobs_queue=range_jobs_queue,
                        warc_records_queue=warc_records_queue,
                        s3_client=s3_client,
                    )
                )
                for i in range(self.num_fetchers)
            ]

            # Write WARC records
            warc_writers = [
                asyncio.create_task(
                    self.write_warc_records(
                        consumer_id=i,
                        warc_records_queue=warc_records_queue,
                        s3_client=s3_client,
                    )
                )
                for i in range(self.num_consumers)
            ]

            await job_generators
            logger.info('Range jobs submitted, waiting for readers to finish')

            await asyncio.gather(*warc_readers)
            logger.info('All WARC readers completed')

            # Send stop signals to consumers
            for _ in range(self.num_consumers):
                await warc_records_queue.put(_STOP)

            consumer_results = await asyncio.gather(*warc_writers)
            n_records = sum([result['stats']['total_requests'] for result in consumer_results])

            logger.info('All WARC writers completed')

        return n_records

    async def generate_range_jobs(
        self,
        range_jobs_queue: asyncio.Queue,
        s3_client=None,
    ):
        """Read the CDX paths, parse lines -> RangeJob (WARC files and offets) -> key_queue."""

        logger.info('Range index limit: %i', self.record_limit)
        count = 0

        # Iterate over index files
        for index_path in self.index_paths:
            # Fetch range queries from index
            try:
                for warc_url, offset, length in iter_cdx_index_from_path(
                    index_path, warc_download_prefix=self.warc_download_prefix
                ):
                    # Convert the CDX record back to a RangeJob
                    job = RangeJob(url=warc_url, offset=offset, length=length)
                    await range_jobs_queue.put(job)
                    count += 1

                    if self.record_limit > 0 and count >= self.record_limit:
                        logger.warning('Index limit reached at %i', count)
                        break

            except Exception as e:
                logger.error('Failed to read CDX index from %s: %s', index_path, e)

            if self.record_limit > 0 and count >= self.record_limit:
                logger.warning('Limit reached at %i', count)
                break

        # signal fetchers to stop
        for _ in range(self.num_fetchers):
            await range_jobs_queue.put(_STOP)

        logger.info('Enqueued %d jobs from %s', count, index_path)

    async def read_warc_records(
        self,
        fetcher_id: int,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ):
        """Read WARC records based on range jobs -> enqueue RangePayload."""
        tracker = ThroughputTracker()
        tracker.start()
        counter = 0

        while True:
            job = await range_jobs_queue.get()
            try:
                if job is _STOP:
                    stats = tracker.get_stats()
                    logger.info(
                        'WARC Fetcher %d stopping. Stats: %.1fs, %d requests, %.1f MB, %.2f MB/s, %.2f req/s',
                        fetcher_id,
                        stats['elapsed'],
                        stats['total_requests'],
                        stats['total_bytes'] / (1024 * 1024),
                        stats['mb_per_sec'],
                        stats['requests_per_sec'],
                    )
                    break  # Exit loop, but still execute finally block
                assert isinstance(job, RangeJob)
                data = await ranged_get_bytes(                    
                    job,
                    self.max_attempts,
                    self.base_backoff_seconds,
                    s3_client=s3_client,
                )
                tracker.add_bytes(len(data))
                counter += 1

                # Log progress every 10 items
                if self.log_every_n > 0 and counter % self.log_every_n == 0:
                    stats = tracker.get_stats()
                    logger.info(
                        'WARC Fetcher %d: %d items, %.1f MB, %.2f MB/s, %.2f req/s',
                        fetcher_id,
                        counter,
                        stats['total_bytes'] / (1024 * 1024),
                        stats['mb_per_sec'],
                        stats['requests_per_sec'],
                    )

                await warc_records_queue.put(RangePayload(job=job, data=data))
            except Exception:
                logger.exception(
                    'WARC Fetcher %d failed on %s/%s [%d,%d]',
                    fetcher_id,
                    getattr(job, 'bucket', '?'),
                    getattr(job, 'key', '?'),
                    getattr(job, 'offset', -1),
                    getattr(job, 'length', -1),
                )
            finally:
                range_jobs_queue.task_done()

    async def write_warc_records(
        self,
        consumer_id: int,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ):
        """Write WARC records. Each consumer owns ONE shard MPU and appends ranges to it."""
        # File rotation tracking
        current_file_sequence = 1
        current_file_size = 0

        new_writer_kwargs = dict(
            s3_client=s3_client,
            consumer_id=consumer_id,
            output_path_prefix=self.prefix_path,
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
            writer_info=self.writer_info,
            warc_version=self.warc_version,
            writer_subprefix=self.writer_subprefix,
            gzip=self.gzip,
            content_type=self.content_type,
            min_part_size=self.min_part_size,
        )

        # Initialize first writer with header
        writer, header_size = await create_new_writer_with_header(
            sequence=current_file_sequence,
            **new_writer_kwargs,
        )
        current_file_size = header_size

        tracker = ThroughputTracker()
        tracker.start()
        counter = 0

        # Write WARC resource records
        if self.write_paths_as_resource_records:
            logger.info(f'Writing {len(self.write_paths_as_resource_records)} resource records to WARC ... ')

            # Resource records are written at the beginning the WARC file.
            for i, resource_record_path in enumerate(self.write_paths_as_resource_records):
                logger.info(f'Writing resource record from {resource_record_path} ...')
                resource_record = get_resource_record_from_path(
                    file_path=resource_record_path,
                    metadata_path=(
                        self.write_paths_as_resource_records_metadata[i]
                        if self.write_paths_as_resource_records_metadata
                        else None
                    ),
                )
                record_data = get_bytes_from_warc_record(
                    resource_record, warc_version=self.warc_version, gzip=self.gzip
                )

                await writer.write(record_data)

                # Keep track but do not rotate resource records
                current_file_size += len(record_data)

            logger.info(f'Resource records added: {len(self.write_paths_as_resource_records)}')

        try:
            while True:
                item = await warc_records_queue.get()
                counter += 1
                try:
                    if item is _STOP:
                        stats = tracker.get_stats()
                        logger.info(
                            'WARC writer %d stopping. Stats: %.1fs, %d items, %.1f MB written, %.2f MB/s write speed',
                            consumer_id,
                            stats['elapsed'],
                            stats['total_requests'],
                            stats['total_bytes'] / (1024 * 1024),
                            stats['mb_per_sec'],
                        )
                        should_stop = True
                    else:
                        should_stop = False
                        assert isinstance(item, RangePayload)

                        # Check if we need to rotate files due to size limit
                        if self.max_file_size and current_file_size + len(item.data) > self.max_file_size:
                            await writer.close()
                            current_file_sequence += 1

                            writer, header_size = await create_new_writer_with_header(
                                sequence=current_file_sequence,
                                **new_writer_kwargs,
                            )

                            current_file_size = header_size
                            logger.info(f'Rotated to new WARC file sequence {current_file_sequence} due to size limit')

                        await writer.write(item.data)
                        current_file_size += len(item.data)
                        tracker.add_bytes(len(item.data))

                        # Log progress every 10 items
                        if self.log_every_n > 0 and counter % self.log_every_n == 0:
                            stats = tracker.get_stats()
                            logger.info(
                                'WARC writer %d: %d items, %.1f MB written, %.2f MB/s',
                                consumer_id,
                                counter,
                                stats['total_bytes'] / (1024 * 1024),
                                stats['mb_per_sec'],
                            )
                except Exception:
                    logger.exception('WARC writer %d failed on %s', consumer_id, getattr(item, 'job', None))
                    should_stop = False
                finally:
                    warc_records_queue.task_done()

                if should_stop:
                    break
        finally:
            await writer.close()

        return {'consumer_id': consumer_id, 'stats': tracker.get_stats()}

    def get_boto3_config(self):
        return Config(
            region_name=self.aws_region_name,
            retries={'max_attempts': max(2, self.max_attempts), 'mode': 'standard'},
            connect_timeout=10,
            read_timeout=120,
        )
