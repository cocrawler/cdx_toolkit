import asyncio
import logging
import statistics
import sys
from typing import List, Literal, Optional, Dict


from botocore.config import Config

from cdx_toolkit.filter_warc.athena_job_generator import get_range_jobs_from_athena
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

TargetSourceType = Literal['cdx', 'athena']


class WARCFilter:
    """Filter or extract specific records from WARC files based on CDX indexes.

    The WARC filter uses a three stage listner-producer-consumer pattern.

    Filter targets:
    - CDX index files from local or remote file system, containing paths to WARC files and positions of target records.

    WARC reader:
    - HTTP range reads
    - S3 range reads

    WARC writer:
    - Local file system
    - S3 using multi-part uploads
    """

    def __init__(
        self,
        prefix_path: str,
        writer_info: Dict,
        target_source: TargetSourceType = 'cdx',
        cdx_paths: Optional[List[str]] = None,
        athena_database: Optional[str] = None,
        athena_hostnames: Optional[List[str]] = None,
        athena_s3_output_location: Optional[str] = None,
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
            target_source: Source of filter targets (Athena query or CDX files).
            cdx_paths: List of paths to CDX index files.
            athena_database: Database for Athena query.
            athena_hostnames: Hostnames for Athena query.
            athena_s3_output_location: S3 output location for Athena query.
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
        self.cdx_paths = cdx_paths
        self.target_source: TargetSourceType = target_source
        self.athena_database = athena_database
        self.athena_s3_output_location = athena_s3_output_location
        self.athena_hostnames = athena_hostnames
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
        self.num_writers = (
            n_parallel_writers
            if n_parallel_writers is not None
            else max(int(self.num_readers / self.fetcher_to_consumer_ratio), 1)
        )

        # self.gzip = self.cdx_paths[0].endswith('.gz') if self.cdx_paths else False
        self.gzip = True

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

    def needs_aws(self) -> bool:
        """Returns true if AWS (S3/Athena) is needed at any stage.

        Returns:
            bool: True if AWS client is needed for any operation.
        """
        return (
            self.target_source == 'athena'  # stage 1
            or (self.cdx_paths is not None and len(self.cdx_paths) > 0 and is_s3_url(self.cdx_paths[0]))  # stage 1
            or is_s3_url(self.warc_download_prefix)  # stage 3
            or is_s3_url(self.prefix_path)  # stage 3
        )

    def get_boto3_base_config(self) -> Dict:
        """Get boto3 base configuration for AWS client.

        Returns:
            Dict: Boto3 base configuration object with retry and timeout settings.
        """
        # Calculate max connections based on parallelism
        # Each reader + writer needs connections, plus some overhead for retries
        # max_pool_connections = max(50, (self.num_readers + self.num_writers) * 2)

        return dict(
            region_name=self.aws_region_name,
            retries={
                'max_attempts': max(2, self.max_attempts),
                'mode': 'adaptive',  # Better than 'standard' for variable workloads
            },
        )

    async def get_aws_clients(self) -> Optional[Dict]:
        """Return S3/Athena clients for job/read/write if needed.

        Returns:
            Optional[aioboto3.Session.client]: S3/Athena client context manager if S3/Athena is needed, None otherwise.

        Raises:
            SystemExit: If S3 is needed but Python version is < 3.9.
        """
        if self.needs_aws():
            if sys.version_info.major < 3 or (sys.version_info.major >= 3 and sys.version_info.minor < 9):
                logger.error('Reading and writing to S3 requires Python version >= 3.9')
                sys.exit(1)

            import aioboto3
            import boto3

            session = aioboto3.Session()

            # Lightweight config for CDX index reads
            job_config = Config(
                max_pool_connections=5,
                read_timeout=60,
                **self.get_boto3_base_config(),
            )

            if self.target_source == 'athena':
                # Athena does not need an async client
                job_client = boto3.client('athena', config=job_config)
            else:
                job_client = session.client('s3', config=job_config)

            # High-throughput config for range reads
            read_config = Config(
                max_pool_connections=self.num_readers * 3,
                read_timeout=300,
                tcp_keepalive=True,
                **self.get_boto3_base_config(),
            )

            # Optimized config for multipart uploads
            write_config = Config(
                max_pool_connections=self.num_writers * 4,
                read_timeout=120,
                connect_timeout=10,
                **self.get_boto3_base_config(),
            )

            return {
                'job': job_client,
                'read': session.client('s3', config=read_config),
                'write': session.client('s3', config=write_config),
            }
        else:
            return None

    async def filter_async(self) -> int:
        """Filter process using a three stage approach (job generator, warc reader, warc writer).

        Returns:
            int: Number of records written.
        """
        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        if self.needs_aws():
            clients = await self.get_aws_clients()

            # Handle mixed async/sync clients - Athena client is sync, S3 clients are async
            if self.target_source == 'athena':
                job_aws_client = clients['job']  # Sync client, no context manager needed
                async with clients['read'] as read_aws_client, clients['write'] as write_aws_client:
                    return await self._run_filter_pipeline(
                        range_jobs_queue=range_jobs_queue,
                        warc_records_queue=warc_records_queue,
                        job_aws_client=job_aws_client,
                        read_s3_client=read_aws_client,
                        write_s3_client=write_aws_client,
                    )
            else:
                async with clients['job'] as job_aws_client, clients['read'] as read_aws_client, clients[
                    'write'
                ] as write_aws_client:
                    return await self._run_filter_pipeline(
                        range_jobs_queue=range_jobs_queue,
                        warc_records_queue=warc_records_queue,
                        job_aws_client=job_aws_client,
                        read_s3_client=read_aws_client,
                        write_s3_client=write_aws_client,
                    )
        else:
            return await self._run_filter_pipeline(
                range_jobs_queue=range_jobs_queue,
                warc_records_queue=warc_records_queue,
            )

    async def _run_filter_pipeline(
        self,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        job_aws_client=None,
        read_s3_client=None,
        write_s3_client=None,
    ) -> int:
        """Run the actual filter pipeline with or without S3 client.

        Args:
            range_jobs_queue: Queue for range jobs from CDX index.
            warc_records_queue: Queue for WARC record payloads.
            job_aws_client: Optional AWS (S3/Athena) client for jobs generation.
            read_s3_client: Optional S3 client for reads from S3.
            write_s3_client: Optional S3 client for writes S3.

        Returns:
            int: Number of records written.
        """
        # Fetch file paths and ranges (offset, length) from index files
        logger.info('Starting job generator, %d WARC readers, %d WARC writers', self.num_readers, self.num_writers)

        # Generate range jobs from different target sources
        if self.target_source == 'cdx':
            job_generators = asyncio.create_task(
                self.generate_range_jobs_from_cdx(
                    range_jobs_queue,
                    s3_client=job_aws_client,
                )
            )
        elif self.target_source == 'athena':
            job_generators = asyncio.create_task(
                get_range_jobs_from_athena(
                    client=job_aws_client,
                    database=self.athena_database,
                    s3_output_location=self.athena_s3_output_location,
                    job_queue=range_jobs_queue,
                    queue_stop_object=_STOP,
                    url_host_names=self.athena_hostnames,
                    warc_download_prefix=self.warc_download_prefix,
                    num_fetchers=self.num_readers,
                    limit=self.record_limit,
                )
            )
        else:
            raise ValueError(f'Invalid target source: {self.target_source}')

        # Read WARC records based on file paths and ranges
        warc_readers = [
            asyncio.create_task(
                self.read_warc_records(
                    reader_id=i,
                    range_jobs_queue=range_jobs_queue,
                    warc_records_queue=warc_records_queue,
                    s3_client=read_s3_client,
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
                    s3_client=write_s3_client,
                )
            )
            for i in range(self.num_writers)
        ]

        # Start writer coordination task
        writer_coordinator = asyncio.create_task(self._coordinate_writer_shutdown(warc_readers, warc_records_queue))

        await job_generators
        logger.info('Range jobs submitted, monitoring readers and writers')

        # Wait for all tasks to complete
        readers_results = await asyncio.gather(*warc_readers)
        writers_results = await asyncio.gather(*warc_writers)
        await writer_coordinator

        readers_records = sum([result['stats']['total_records'] for result in readers_results])
        readers_mb_per_sec = self.num_readers * statistics.mean(
            [result['stats']['mb_per_sec'] for result in readers_results]
        )
        readers_records_per_sec = self.num_readers * statistics.mean(
            [result['stats']['records_per_sec'] for result in readers_results]
        )

        logger.info(f'All WARC readers completed: {readers_records} records')
        logger.info(f'Total reader throughput: {readers_mb_per_sec:.2f} MB/s; {readers_records_per_sec:.2f} rec/s')

        writers_records = sum([result['stats']['total_records'] for result in writers_results])
        writers_mb_per_sec = self.num_writers * statistics.mean(
            [result['stats']['mb_per_sec'] for result in writers_results]
        )
        writers_records_per_sec = self.num_writers * statistics.mean(
            [result['stats']['records_per_sec'] for result in writers_results]
        )

        logger.info(f'All WARC writers completed: {writers_records} records')
        logger.info(f'Total writer throughput: {writers_mb_per_sec:.2f} MB/s; {writers_records_per_sec:.2f} rec/s')

        return writers_records

    async def _coordinate_writer_shutdown(self, warc_readers: List[asyncio.Task], warc_records_queue: asyncio.Queue):
        """Coordinate efficient shutdown of writers as readers complete.

        This prevents writers from waiting unnecessarily when all readers are done
        and the records queue is being drained.
        """
        completed_readers = 0

        # Monitor reader completion
        while completed_readers < len(warc_readers):
            # Wait for any reader to complete
            done, pending = await asyncio.wait(
                warc_readers,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0,  # Check periodically
            )

            if done:
                completed_readers = len(warc_readers) - len(pending)
                logger.debug(f'Readers completed: {completed_readers}/{len(warc_readers)}')

        # All readers completed - signal writers to stop
        logger.info('All readers completed, signaling writers to stop')

        # Send stop signals to all writers
        for _ in range(self.num_writers):
            await warc_records_queue.put(_STOP)

    async def generate_range_jobs_from_single_cdx(
        self,
        cdx_path: str,
        range_jobs_queue: asyncio.Queue,
        count: int = 0,
    ) -> int:
        """Read a CDX file and generate range jobs based on URLs and offsets."""
        for warc_url, offset, length in iter_cdx_index_from_path(
            cdx_path, warc_download_prefix=self.warc_download_prefix
        ):
            # Convert the CDX record back to a RangeJob
            job = RangeJob(url=warc_url, offset=offset, length=length, records_count=1)
            await range_jobs_queue.put(job)
            count += 1

            if self.record_limit > 0 and count >= self.record_limit:
                logger.warning('Index limit reached at %i', count)
                break

        return count

    async def generate_range_jobs_from_cdx(
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
        # TODO this could be done in parallel
        for index_path in self.cdx_paths:
            # Fetch range queries from index
            try:
                count += await self.generate_range_jobs_from_single_cdx(
                    cdx_path=index_path,
                    range_jobs_queue=range_jobs_queue,
                    count=count,
                )

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
                self.log_reader(reader_id=reader_id, counter=counter, tracker=tracker)

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

    async def write_resource_records(self, writer, warcinfo_id: str) -> int:
        """Write WARC resource records based on paths"""
        resource_records_size = 0

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
                warcinfo_id=warcinfo_id,
            )
            record_data = get_bytes_from_warc_record(resource_record, warc_version=self.warc_version, gzip=self.gzip)
            await writer.write(record_data)

            # Keep track but do not rotate resource records
            resource_records_size += len(record_data)

        logger.info(f'Resource records added: {len(self.write_paths_as_resource_records)}')

        return resource_records_size

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
        writer, header_size, warcinfo_id = await create_new_writer_with_header(
            sequence=current_file_sequence,
            **new_writer_kwargs,
        )
        current_file_size = header_size

        tracker = ThroughputTracker()
        tracker.start()
        counter = 0

        # Resource records
        if self.write_paths_as_resource_records:
            current_file_size += await self.write_resource_records(writer, warcinfo_id=warcinfo_id)

        # Response records
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
                        writer, current_file_sequence, current_file_size = await self.rotate_files(
                            writer=writer,
                            current_file_sequence=current_file_sequence,
                            current_file_size=current_file_size,
                            added_byte_size=len(item.data),
                            **new_writer_kwargs,
                        )

                        # Write actual response record
                        await writer.write(item.data)
                        current_file_size += len(item.data)
                        tracker.add(bytes_count=len(item.data), records_count=item.job.records_count)

                        # Log progress every N items
                        self.log_writer(writer_id=writer_id, counter=counter, tracker=tracker)

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

    def log_reader(self, reader_id: int, counter: int, tracker: ThroughputTracker):
        """Log progress every N items."""
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

    def log_writer(self, writer_id: int, counter: int, tracker: ThroughputTracker):
        """Log progress every N items."""
        if self.log_every_n > 0 and counter % self.log_every_n == 0:
            stats = tracker.get_stats()
            logger.info(
                'WARC Writer %d: %d items, %.1f MB written, %.2f MB/s',
                writer_id,
                counter,
                stats['total_bytes'] / (1024 * 1024),
                stats['mb_per_sec'],
            )

    async def rotate_files(
        self, writer, current_file_sequence: int, current_file_size: int, added_byte_size: int, **new_writer_kwargs
    ):
        """Check if we need to rotate files due to size limit and perform rotation if needed."""
        if self.max_file_size and current_file_size + added_byte_size > self.max_file_size:
            await writer.close()
            current_file_sequence += 1

            writer, header_size, warcinfo_id = await create_new_writer_with_header(
                sequence=current_file_sequence,
                **new_writer_kwargs,
            )

            current_file_size = header_size
            logger.info(f'Rotated to new WARC file sequence {current_file_sequence} due to size limit')

            # Resource records also to new files
            if self.write_paths_as_resource_records:
                current_file_size += await self.write_resource_records(writer, warcinfo_id=warcinfo_id)

        return writer, current_file_sequence, current_file_size
