import asyncio
import logging
import os
import statistics
import sys
from typing import List, Optional, Dict


from botocore.config import Config

from cdx_toolkit.filter_warc.s3_utils import (
    is_s3_url,
)
from cdx_toolkit.filter_warc.data_classes import RangeJob, RangePayload, ThroughputTracker
from cdx_toolkit.filter_warc.warc_utils import create_new_writer_with_header
from cdx_toolkit.filter_warc.sources.base import RangeJobSource
from cdx_toolkit.filter_warc.sources.csv import RangeJobCsvWriter
from cdx_toolkit.filter_warc.warc_utils import get_bytes_from_warc_record, get_metadata_record_from_path


_STOP = object()

logger = logging.getLogger(__name__)


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
        source: RangeJobSource,
        range_jobs_output: Optional[str] = None,
        no_fetch: bool = False,
        csv_self_contained: bool = False,
        writer_subprefix: Optional[str] = None,
        write_paths_as_metadata_records: Optional[List[str]] = None,
        record_limit: int = 0,
        log_every_n: int = 1000,
        warc_download_prefix: Optional[str] = None,
        n_parallel: int = 1,
        n_parallel_readers: Optional[int] = None,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
        # writer_kwargs: Optional[Dict] = None,
        range_jobs_queue_size: int = 1000,
        warc_records_queue_size: int = 200,
        aws_region_name: str = 'us-east-1',
        warc_version: str = '1.0',
        content_type: Optional[str] = None,
        min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
        max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
        warcinfo_record_id: Optional[str] = None,
        warcinfo_filename: Optional[str] = None,
        write_warcinfo: bool = True,
    ):
        """Initialize the WARC filter.

        Args:
            source: RangeJobSource that yields the WARC ranges to repackage.
            range_jobs_output: Optional path; if set, each generated RangeJob is
                written to this CSV (materialization).
            no_fetch: If True, only generate range jobs (and write range_jobs_output);
                skip fetching/writing WARC records entirely.
            csv_self_contained: If True, range_jobs_output stores full URLs instead of
                relative filenames.
            prefix_path: Output path prefix for filtered WARC files.
            writer_info: Dictionary containing writer metadata.
            writer_subprefix: Optional subprefix for writer output paths.
            write_paths_as_metadata_records: Optional list of file paths to write as metadata records.
            record_limit: Maximum number of records to process (0 for unlimited).
            log_every_n: Log progress every N records.
            warc_download_prefix: Optional prefix to prepend to WARC URLs.
            n_parallel: Number of async readers per process (one writer per process).
            n_parallel_readers: Number of async reader tasks (overrides n_parallel).
            max_attempts: Maximum retry attempts for failed operations.
            base_backoff_seconds: Base backoff time in seconds for retries.
            writer_kwargs: Optional additional kwargs for writers.
            range_jobs_queue_size: Maximum size of range jobs queue.
            warc_records_queue_size: Maximum size of WARC records queue.
            aws_region_name: AWS region name for S3 operations.
            warc_version: WARC format version (e.g., '1.0' or '1.1').
            content_type: Optional content type for WARC output.
            min_part_size: Minimum part byte size for multipart uploads (default: 5 MiB).
            max_file_size: Maximum byte size for individual WARC files (default: 1 GiB).
        """
        self.source = source
        self.range_jobs_output = range_jobs_output
        self.no_fetch = no_fetch
        self.csv_self_contained = csv_self_contained
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.write_paths_as_metadata_records = write_paths_as_metadata_records
        self.record_limit = record_limit
        self.log_every_n = log_every_n
        self.warc_download_prefix = warc_download_prefix

        # self.writer_kwargs = writer_kwargs
        self.range_jobs_queue_size = range_jobs_queue_size
        self.warc_records_queue_size = warc_records_queue_size
        self.aws_region_name = aws_region_name
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds

        # Many async readers feed a single writer. Writing is a cheap verbatim byte copy
        # (+ >=5 MiB MPU parts) that one coroutine sustains easily; multi-core scaling and
        # output sharding are handled by running multiple *processes* (see
        # filter_warc/multiprocess.py), not multiple writers on one event loop.
        self.n_parallel = n_parallel
        self.num_readers = n_parallel_readers if n_parallel_readers is not None else n_parallel

        self.gzip = True

        self.warc_version = warc_version
        self.content_type = content_type
        self.min_part_size = min_part_size
        self.max_file_size = max_file_size
        self.warcinfo_record_id = warcinfo_record_id
        self.warcinfo_filename = warcinfo_filename
        self.write_warcinfo = write_warcinfo

    def filter(self) -> int:
        """Perform the filtering process (calls async method via asyncio.run).

        Returns:
            int: Number of records written, or -1 if interrupted.
        """
        runner = asyncio.run
        if os.environ.get('CDXT_UVLOOP') == '1':
            try:
                import uvloop

                runner = uvloop.run
                logger.info('Using uvloop event loop')
            except ImportError:
                logger.warning('CDXT_UVLOOP=1 but uvloop is not installed; using default asyncio loop')
        try:
            return runner(self.filter_async())
        except KeyboardInterrupt:
            logger.warning('Interrupted by user.')

        return -1

    def needs_aws(self) -> bool:
        """Returns true if the read/write (stage 2/3) S3 clients are needed.

        Sources own their own stage-1 resource (Athena client / DuckDB connection /
        fsspec), so this only concerns WARC reads and output writes. With no_fetch
        there are no reads/writes at all.
        """
        if self.no_fetch:
            return False
        return is_s3_url(self.warc_download_prefix) or is_s3_url(self.prefix_path)

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
        """Return async S3 clients for WARC reads/writes if needed.

        Stage-1 clients/connections are owned by the source, so this only builds the
        read/write S3 clients used to fetch WARC ranges and write output.

        Raises:
            SystemExit: If S3 is needed but Python version is < 3.9.
        """
        if self.needs_aws():
            if sys.version_info.major < 3 or (sys.version_info.major >= 3 and sys.version_info.minor < 9):
                logger.error('Reading and writing to S3 requires Python version >= 3.9')
                sys.exit(1)

            import aioboto3

            session = aioboto3.Session()

            # High-throughput config for range reads
            read_config = Config(
                max_pool_connections=self.num_readers * 3,
                read_timeout=300,
                tcp_keepalive=True,
                **self.get_boto3_base_config(),
            )

            # Optimized config for multipart uploads (single writer)
            write_config = Config(
                max_pool_connections=8,
                read_timeout=120,
                connect_timeout=10,
                **self.get_boto3_base_config(),
            )

            return {
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
        # Materialize-only: just drain the source into the range-jobs CSV.
        if self.no_fetch:
            return await self._run_materialize_only()

        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        if self.needs_aws():
            clients = await self.get_aws_clients()
            async with clients['read'] as read_aws_client, clients['write'] as write_aws_client:
                return await self._run_filter_pipeline(
                    range_jobs_queue=range_jobs_queue,
                    warc_records_queue=warc_records_queue,
                    read_s3_client=read_aws_client,
                    write_s3_client=write_aws_client,
                )
        else:
            return await self._run_filter_pipeline(
                range_jobs_queue=range_jobs_queue,
                warc_records_queue=warc_records_queue,
            )

    def _make_csv_writer(self) -> Optional[RangeJobCsvWriter]:
        if self.range_jobs_output is None:
            return None
        return RangeJobCsvWriter(self.range_jobs_output, self_contained=self.csv_self_contained)

    async def _produce_range_jobs(self, range_jobs_queue: Optional[asyncio.Queue], csv_writer) -> int:
        """Drive the (sync) source in a worker thread, feeding the async queue.

        Owns counting, the record limit, and (when a queue is present) emitting one
        _STOP sentinel per reader in a finally -- so readers never hang even if the
        source raises mid-iteration."""
        loop = asyncio.get_running_loop()
        count = 0

        def drain() -> int:
            nonlocal count
            for job in self.source.iter_range_jobs():
                if csv_writer is not None:
                    csv_writer.write(job)
                if range_jobs_queue is not None:
                    asyncio.run_coroutine_threadsafe(range_jobs_queue.put(job), loop).result()
                count += 1
                if self.record_limit and count >= self.record_limit:
                    logger.warning('Limit reached at %i', count)
                    break
            return count

        try:
            await asyncio.to_thread(drain)
        finally:
            if csv_writer is not None:
                csv_writer.close()
            if range_jobs_queue is not None:
                for _ in range(self.num_readers):
                    await range_jobs_queue.put(_STOP)

        logger.info('Generated %d range jobs', count)
        return count

    async def _run_materialize_only(self) -> int:
        """--no-fetch: generate range jobs and write only the range-jobs CSV."""
        csv_writer = self._make_csv_writer()
        if csv_writer is None:
            logger.warning('--no-fetch set without --range-jobs-output: nothing to do')
        count = await self._produce_range_jobs(range_jobs_queue=None, csv_writer=csv_writer)
        logger.info('Materialized %d range jobs (no WARC fetch)', count)
        return count

    async def _run_filter_pipeline(
        self,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        read_s3_client=None,
        write_s3_client=None,
    ) -> int:
        """Run the actual filter pipeline with or without S3 client.

        Args:
            range_jobs_queue: Queue for range jobs from the source.
            warc_records_queue: Queue for WARC record payloads.
            read_s3_client: Optional S3 client for reads from S3.
            write_s3_client: Optional S3 client for writes S3.

        Returns:
            int: Number of records written.
        """
        logger.info('Starting job generator, %d WARC readers, 1 WARC writer', self.num_readers)

        # Generate range jobs from the configured source (bridged sync->async in a thread).
        csv_writer = self._make_csv_writer()
        job_generators = asyncio.create_task(self._produce_range_jobs(range_jobs_queue, csv_writer))

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

        # Write WARC records (a single writer owns the output shard for this process)
        warc_writer = asyncio.create_task(
            self.write_warc_records(
                warc_records_queue=warc_records_queue,
                s3_client=write_s3_client,
            )
        )

        # Start writer coordination task
        writer_coordinator = asyncio.create_task(self._coordinate_writer_shutdown(warc_readers, warc_records_queue))

        await job_generators
        logger.info('Range jobs submitted, monitoring readers and writer')

        # Wait for all tasks to complete
        readers_results = await asyncio.gather(*warc_readers)
        writer_result = await warc_writer
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

        writer_stats = writer_result['stats']
        logger.info(f"WARC writer completed: {writer_stats['total_records']} records")
        logger.info(
            f"Total writer throughput: {writer_stats['mb_per_sec']:.2f} MB/s; "
            f"{writer_stats['records_per_sec']:.2f} rec/s"
        )

        return writer_stats['total_records']

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

        # All readers completed - signal the writer to stop
        logger.info('All readers completed, signaling writer to stop')
        await warc_records_queue.put(_STOP)

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

    async def write_metadata_records(self, writer, warcinfo_id: str) -> int:
        """Write WARC metadata records based on paths"""
        records_size = 0
        records_count = 0

        logger.info(f'Writing {len(self.write_paths_as_metadata_records)} metadata records to WARC ... ')

        # Metadata records are written at the beginning each WARC file.
        for i, resource_record_path in enumerate(self.write_paths_as_metadata_records):
            logger.info(f'Writing metadata record from {resource_record_path} ...')
            record = get_metadata_record_from_path(
                file_path=resource_record_path,
                warcinfo_id=warcinfo_id,
            )
            record_data = get_bytes_from_warc_record(record, warc_version=self.warc_version, gzip=self.gzip)
            await writer.write(record_data)

            # Keep track but do not rotate resource records
            records_size += len(record_data)
            records_count += 1

        logger.info(f'Metadata records added: {records_count}')

        return records_size

    async def write_warc_records(
        self,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ) -> dict:
        """Write WARC records. The single writer owns the output WARC (one shard per
        process) and appends ranges to it, rotating by --size when set.

        Args:
            warc_records_queue: Queue to read RangePayload objects from.
            s3_client: Optional S3 client for writing WARC files to S3.

        Returns:
            dict: Statistics dictionary with throughput stats.
        """
        # File rotation tracking
        current_file_sequence = 1
        current_file_size = 0

        new_writer_kwargs = dict(
            s3_client=s3_client,
            output_path_prefix=self.prefix_path,
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
            writer_info=self.writer_info,
            warc_version=self.warc_version,
            writer_subprefix=self.writer_subprefix,
            gzip=self.gzip,
            content_type=self.content_type,
            min_part_size=self.min_part_size,
            warcinfo_record_id=self.warcinfo_record_id,
            warcinfo_filename=self.warcinfo_filename,
            write_warcinfo=self.write_warcinfo,
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
        if self.write_paths_as_metadata_records:
            current_file_size += await self.write_metadata_records(writer, warcinfo_id=warcinfo_id)

        # Response records
        try:
            while True:
                item = await warc_records_queue.get()
                counter += 1
                try:
                    if item is _STOP:
                        stats = tracker.get_stats()
                        logger.info(
                            'WARC writer stopping. Stats: %.1fs, %d items, %.1f MB written, %.2f MB/s write speed',
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
                        self.log_writer(counter=counter, tracker=tracker)

                except Exception:
                    logger.exception('WARC writer failed on %s', getattr(item, 'job', None))
                    should_stop = False
                finally:
                    warc_records_queue.task_done()

                if should_stop:
                    break
        finally:
            await writer.close()

        return {'stats': tracker.get_stats()}

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

    def log_writer(self, counter: int, tracker: ThroughputTracker):
        """Log progress every N items."""
        if self.log_every_n > 0 and counter % self.log_every_n == 0:
            stats = tracker.get_stats()
            logger.info(
                'WARC Writer: %d items, %.1f MB written, %.2f MB/s',
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
            if self.write_paths_as_metadata_records:
                current_file_size += await self.write_metadata_records(writer, warcinfo_id=warcinfo_id)

        return writer, current_file_sequence, current_file_size
