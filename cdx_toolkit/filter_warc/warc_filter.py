import asyncio
import logging
import statistics
import sys
from typing import List, Optional, Dict


from botocore.config import Config

from cdx_toolkit.filter_warc.s3_utils import (
    is_s3_url,
)
from cdx_toolkit.filter_warc.data_classes import RangeJob, RangePayload, ThroughputTracker
from cdx_toolkit.filter_warc.warc_utils import create_new_writer_with_header
from cdx_toolkit.filter_warc.cdx_utils import (
    iter_cdx_index_from_path,
)
from cdx_toolkit.filter_warc.warc_utils import get_bytes_from_warc_record, get_resource_record_from_path


_STOP = object()

logger = logging.getLogger(__name__)


class WARCFilter:
    """Filter or extract specific records from WARC files based on CDX indexes.
    
    The WARC filter uses a three stage listner-producer-consumer pattern.

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
        n_parallel_readers: Optional[int] = None,
        n_parallel_writers: Optional[int] = None,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
        # writer_kwargs: Optional[Dict] = None,
        range_jobs_queue_size: int = 1000,
        warc_records_queue_size: int = 200,
        fetcher_to_consumer_ratio: int = 6,
        aws_region_name: str = 'us-east-1',
        warc_version: str = '1.0',
        content_type: Optional[str] = None,
        min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
        max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
    ):
        """Initialize the WARC filter.

        Args:
            index_paths: List of paths to CDX index files.
            prefix_path: Output path prefix for filtered WARC files.
            writer_info: Dictionary containing writer metadata.
            writer_subprefix: Optional subprefix for writer output paths.
            write_paths_as_resource_records: Optional list of file paths to write as resource records.
            write_paths_as_resource_records_metadata: Optional list of metadata paths for resource records.
            record_limit: Maximum number of records to process (0 for unlimited).
            log_every_n: Log progress every N records.
            warc_download_prefix: Optional prefix to prepend to WARC URLs.
            n_parallel: Number of parallel workers (default for readers/writers).
            n_parallel_readers: Number of parallel reader tasks (overrides n_parallel).
            n_parallel_writers: Number of parallel writer tasks (overrides n_parallel).
            max_attempts: Maximum retry attempts for failed operations.
            base_backoff_seconds: Base backoff time in seconds for retries.
            writer_kwargs: Optional additional kwargs for writers.
            range_jobs_queue_size: Maximum size of range jobs queue.
            warc_records_queue_size: Maximum size of WARC records queue.
            fetcher_to_consumer_ratio: Ratio of readers to writers for auto-scaling.
            aws_region_name: AWS region name for S3 operations.
            warc_version: WARC format version (e.g., '1.0' or '1.1').
            content_type: Optional content type for WARC output.
            min_part_size: Minimum part byte size for multipart uploads (default: 5 MiB).
            max_file_size: Maximum byte size for individual WARC files (default: 1 GiB).
        """
        self.index_paths = index_paths
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.write_paths_as_resource_records = write_paths_as_resource_records
        self.write_paths_as_resource_records_metadata = write_paths_as_resource_records_metadata
        self.record_limit = record_limit
        self.log_every_n = log_every_n
        self.warc_download_prefix = warc_download_prefix

        # self.writer_kwargs = writer_kwargs
        self.range_jobs_queue_size = range_jobs_queue_size
        self.warc_records_queue_size = warc_records_queue_size
        self.aws_region_name = aws_region_name
        self.fetcher_to_consumer_ratio = fetcher_to_consumer_ratio
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds

        self.n_parallel = n_parallel
        self.num_readers = n_parallel_readers if n_parallel_readers is not None else n_parallel
        self.num_writers = n_parallel_writers if n_parallel_writers is not None else max(int(self.num_readers / self.fetcher_to_consumer_ratio), 1)

        self.gzip = self.index_paths[0].endswith('.gz') if self.index_paths else False
        self.warc_version = warc_version
        self.content_type = content_type
        self.min_part_size = min_part_size
        self.max_file_size = max_file_size

    def filter(self) -> int:
        """Perform the filtering process (calls async method via asyncio.run).

        Returns:
            int: Number of records written, or -1 if interrupted.
        """
        try:
            return asyncio.run(self.filter_async())
        except KeyboardInterrupt:
            logger.warning('Interrupted by user.')

        return -1

    def needs_s3(self) -> bool:
        """Returns true if S3 is needed at any stage.

        Returns:
            bool: True if S3 client is needed for any operation.
        """
        return (
            (self.index_paths is not None and len(self.index_paths) > 0 and is_s3_url(self.index_paths[0]))  # stage 1
            or is_s3_url(self.warc_download_prefix)  # stage 3
            or is_s3_url(self.prefix_path)  # stage 3
        )

    def get_s3_client_context(self):
        """Return s3 client context if needed.

        Returns:
            Optional[aioboto3.Session.client]: S3 client context manager if S3 is needed, None otherwise.

        Raises:
            SystemExit: If S3 is needed but Python version is < 3.9.
        """
        if self.needs_s3():
            if sys.version_info.major < 3 or (sys.version_info.major >= 3 and sys.version_info.minor < 9):
                logger.error('Reading and writing to S3 requires Python version >= 3.9')
                sys.exit(1)

            import aioboto3

            session = aioboto3.Session()

            return session.client('s3', config=self.get_boto3_config())
        else:
            return None

    async def filter_async(self) -> int:
        """Filter process using a three stage approach (job generator, warc reader, warc writer).

        Returns:
            int: Number of records written.
        """
        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        s3_client_context = self.get_s3_client_context()
        if s3_client_context is not None:
            async with s3_client_context as s3_client:
                return await self._run_filter_pipeline(range_jobs_queue, warc_records_queue, s3_client)
        else:
            return await self._run_filter_pipeline(range_jobs_queue, warc_records_queue)

    async def _run_filter_pipeline(
            self,
            range_jobs_queue: asyncio.Queue,
            warc_records_queue: asyncio.Queue,
            s3_client=None,
        ) -> int:
        """Run the actual filter pipeline with or without S3 client.

        Args:
            range_jobs_queue: Queue for range jobs from CDX index.
            warc_records_queue: Queue for WARC record payloads.
            s3_client: Optional S3 client for reading/writing to S3.

        Returns:
            int: Number of records written.
        """
        # Fetch file paths and ranges (offset, length) from index files
        logger.info('Starting lister, %d fetchers, %d consumers', self.num_readers, self.num_writers)

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
                    reader_id=i,
                    range_jobs_queue=range_jobs_queue,
                    warc_records_queue=warc_records_queue,
                    s3_client=s3_client,
                )
            )
            for i in range(self.num_readers)
        ]

        # Write WARC records
        warc_writers = [
            asyncio.create_task(
                self.write_warc_records(
                    writer_id=i,
                    warc_records_queue=warc_records_queue,
                    s3_client=s3_client,
                )
            )
            for i in range(self.num_writers)
        ]

        await job_generators
        logger.info('Range jobs submitted, waiting for readers to finish')

        readers_results = await asyncio.gather(*warc_readers)

        readers_records = sum([result['stats']['total_records'] for result in readers_results])
        readers_mb_per_sec = statistics.mean([result['stats']['mb_per_sec'] for result in readers_results])
        readers_records_per_sec = statistics.mean(
            [result['stats']['records_per_sec'] for result in readers_results]
        )

        logger.info(f'All WARC readers completed: {readers_records} records')
        logger.info(f'Reader throughput: {readers_mb_per_sec:.2f} MB/s; {readers_records_per_sec:.2f} rec/s')

        # Send stop signals to consumers
        for _ in range(self.num_writers):
            await warc_records_queue.put(_STOP)

        writers_results = await asyncio.gather(*warc_writers)

        writers_records = sum([result['stats']['total_records'] for result in writers_results])
        writers_mb_per_sec = statistics.mean([result['stats']['mb_per_sec'] for result in writers_results])
        writers_records_per_sec = statistics.mean(
            [result['stats']['records_per_sec'] for result in writers_results]
        )

        logger.info(f'All WARC writers completed: {writers_records} records')
        logger.info(f'Writer throughput: {writers_mb_per_sec:.2f} MB/s; {writers_records_per_sec:.2f} rec/s')

        return writers_records

    async def generate_range_jobs(
        self,
        range_jobs_queue: asyncio.Queue,
        s3_client=None,
    ):
        """Read the CDX paths, parse lines -> RangeJob (WARC files and offets) -> key_queue.

        Args:
            range_jobs_queue: Queue to put RangeJob objects into.
            s3_client: Optional S3 client for reading CDX indexes from S3.
        """

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
                    job = RangeJob(url=warc_url, offset=offset, length=length, records_count=1)
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
        for _ in range(self.num_readers):
            await range_jobs_queue.put(_STOP)

        logger.info('Enqueued %d jobs from %s', count, index_path)

    async def read_warc_records(
        self,
        reader_id: int,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ) -> dict:
        """Read WARC records based on range jobs -> enqueue RangePayload.

        Args:
            reader_id: Unique identifier for this reader task.
            range_jobs_queue: Queue to read RangeJob objects from.
            warc_records_queue: Queue to put RangePayload objects into.
            s3_client: Optional S3 client for reading WARC files from S3.

        Returns:
            dict: Statistics dictionary with reader_id and throughput stats.
        """
        tracker = ThroughputTracker()
        tracker.start()
        counter = 0

        while True:
            job = await range_jobs_queue.get()
            try:
                if job is _STOP:
                    stats = tracker.get_stats()
                    logger.info(
                        'WARC Reader %d stopping. Stats: %.1fs, %d requests, %.1f MB, %.2f MB/s, %.2f req/s',
                        reader_id,
                        stats['elapsed'],
                        stats['total_requests'],
                        stats['total_bytes'] / (1024 * 1024),
                        stats['mb_per_sec'],
                        stats['requests_per_sec'],
                    )
                    break  # Exit loop, but still execute finally block
                assert isinstance(job, RangeJob)
                data = await job.ranged_get_bytes(
                    self.max_attempts,
                    self.base_backoff_seconds,
                    s3_client=s3_client,
                )
                tracker.add(bytes_count=len(data), records_count=job.records_count)
                counter += 1

                # Log progress every N items
                if self.log_every_n > 0 and counter % self.log_every_n == 0:
                    stats = tracker.get_stats()
                    logger.info(
                        'WARC Reader %d: %d items, %.1f MB, %.2f MB/s, %.2f req/s',
                        reader_id,
                        counter,
                        stats['total_bytes'] / (1024 * 1024),
                        stats['mb_per_sec'],
                        stats['requests_per_sec'],
                    )

                await warc_records_queue.put(RangePayload(job=job, data=data))
            except Exception:
                logger.exception(
                    'WARC Reader %d failed on %s/%s [%d,%d]',
                    reader_id,
                    getattr(job, 'bucket', '?'),
                    getattr(job, 'key', '?'),
                    getattr(job, 'offset', -1),
                    getattr(job, 'length', -1),
                )
            finally:
                range_jobs_queue.task_done()

        return {'reader_id': reader_id, 'stats': tracker.get_stats()}

    async def write_warc_records(
        self,
        writer_id: int,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ) -> dict:
        """Write WARC records. Each writer owns ONE shard MPU and appends ranges to it.

        Args:
            writer_id: Unique identifier for this writer task.
            warc_records_queue: Queue to read RangePayload objects from.
            s3_client: Optional S3 client for writing WARC files to S3.

        Returns:
            dict: Statistics dictionary with writer_id and throughput stats.
        """
        # File rotation tracking
        current_file_sequence = 1
        current_file_size = 0

        new_writer_kwargs = dict(
            s3_client=s3_client,
            writer_id=writer_id,
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
                            writer_id,
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
                        tracker.add(bytes_count=len(item.data), records_count=item.job.records_count)

                        # Log progress every 10 items
                        if self.log_every_n > 0 and counter % self.log_every_n == 0:
                            stats = tracker.get_stats()
                            logger.info(
                                'WARC writer %d: %d items, %.1f MB written, %.2f MB/s',
                                writer_id,
                                counter,
                                stats['total_bytes'] / (1024 * 1024),
                                stats['mb_per_sec'],
                            )
                except Exception:
                    logger.exception('WARC writer %d failed on %s', writer_id, getattr(item, 'job', None))
                    should_stop = False
                finally:
                    warc_records_queue.task_done()

                if should_stop:
                    break
        finally:
            await writer.close()

        return {'writer_id': writer_id, 'stats': tracker.get_stats()}

    def get_boto3_config(self):
        """Get boto3 configuration for S3 client.

        Returns:
            Config: Boto3 configuration object with retry and timeout settings.
        """
        # Calculate max connections based on parallelism
        # Each reader + writer needs connections, plus some overhead for retries
        max_pool_connections = max(50, (self.num_readers + self.num_writers) * 2)

        return Config(
            region_name=self.aws_region_name,
            retries={'max_attempts': max(2, self.max_attempts), 'mode': 'standard'},
            connect_timeout=10,
            read_timeout=120,
            max_pool_connections=max_pool_connections,
        )
