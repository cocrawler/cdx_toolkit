import asyncio
from io import BytesIO
import logging
from typing import List, Optional, Dict

import aioboto3
from botocore.config import Config
from warcio import WARCWriter

from cdx_toolkit.warcer_by_cdx.aioboto3_utils import (
    RangeJob,
    RangePayload,
    ThroughputTracker,
    parse_s3_uri,
    ranged_get_bytes,
)
from cdx_toolkit.warcer_by_cdx.aioboto3_writer import ShardWriter
from cdx_toolkit.warcer_by_cdx.cdx_utils import (
    iter_cdx_index_from_path,
)
from cdx_toolkit.warcer_by_cdx.warc_utils import get_bytes_from_warc_record, get_resource_record_from_path


_STOP = object()

logger = logging.getLogger(__name__)


def filter_warc_by_cdx_via_aioboto3(
    index_paths: List[str],
    prefix_path: str,
    writer_info: Dict,
    writer_subprefix: Optional[str] = None,
    write_paths_as_resource_records: Optional[List[str]] = None,
    write_paths_as_resource_records_metadata: Optional[List[str]] = None,
    limit: int = 0,
    log_every_n: int = 1000,
    warc_download_prefix: Optional[str] = None,
    n_parallel: int = 1,
    writer_kwargs: Optional[Dict] = None,
) -> int:
    try:
        return asyncio.run(
            filter_warc_by_cdx_via_aioboto3_async(
                index_paths=index_paths,
                prefix_path=prefix_path,
                writer_info=writer_info,
                writer_subprefix=writer_subprefix,
                write_paths_as_resource_records=write_paths_as_resource_records,
                write_paths_as_resource_records_metadata=write_paths_as_resource_records_metadata,
                limit=limit,
                log_every_n=log_every_n,
                warc_download_prefix=warc_download_prefix,
                writer_kwargs=writer_kwargs,
                n_parallel=n_parallel,
            )
        )
    except KeyboardInterrupt:
        logger.warning('Interrupted by user.')

    return -1


async def filter_warc_by_cdx_via_aioboto3_async(
    index_paths: List[str],
    prefix_path: str,
    writer_info: Dict,
    writer_subprefix: Optional[str] = None,
    write_paths_as_resource_records: Optional[List[str]] = None,
    write_paths_as_resource_records_metadata: Optional[List[str]] = None,
    limit: int = 0,
    log_every_n: int = 1000,
    warc_download_prefix: Optional[str] = None,
    n_parallel: int = 1,
    writer_kwargs: Optional[Dict] = None,
    max_attempts: int = 5,
    key_queue_size: int = 1000,
    item_queue_size: int = 200,
    base_backoff_seconds=0.5,
    s3_region_name: str = 'us-east-1',
) -> int:
    n_records = 0
    fetcher_to_consumer_ratio = 6
    num_fetchers = n_parallel
    num_consumers = max(int(num_fetchers / fetcher_to_consumer_ratio), 1)

    key_queue: asyncio.Queue = asyncio.Queue(maxsize=key_queue_size)
    item_queue: asyncio.Queue = asyncio.Queue(maxsize=item_queue_size)

    boto_cfg = Config(
        region_name=s3_region_name,
        retries={'max_attempts': max(2, max_attempts), 'mode': 'standard'},
        connect_timeout=10,
        read_timeout=120,
    )

    session = aioboto3.Session()

    async with session.client('s3', config=boto_cfg) as s3:
        # Fetch file paths and ranges (offset, length) from index files
        logger.info('Starting lister, %d fetchers, %d consumers', num_fetchers, num_consumers)
        lister_task = asyncio.create_task(
            get_range_jobs_from_index_paths(
                key_queue=key_queue,
                index_paths=index_paths,
                warc_download_prefix=warc_download_prefix,
                num_fetchers=num_fetchers,
                limit=limit,
            )
        )

        # Read WARC records based on file paths and ranges
        fetchers = [
            asyncio.create_task(
                fetch_warc_ranges(
                    fetcher_id=i,
                    key_queue=key_queue,
                    item_queue=item_queue,
                    s3=s3,
                    max_attempts=max_attempts,
                    base_backoff_seconds=base_backoff_seconds,
                    log_every_n=log_every_n,
                )
            )
            for i in range(num_fetchers)
        ]

        # Write WARC records
        consumers = [
            asyncio.create_task(
                write_warc(
                    consumer_id=i,
                    item_queue=item_queue,
                    s3=s3,
                    prefix_path=prefix_path,
                    max_attempts=max_attempts,
                    base_backoff_seconds=base_backoff_seconds,
                    write_paths_as_resource_records=write_paths_as_resource_records,
                    write_paths_as_resource_records_metadata=write_paths_as_resource_records_metadata,
                    writer_info=writer_info,
                    writer_subprefix=writer_subprefix,
                    writer_kwargs=writer_kwargs,
                    log_every_n=log_every_n,
                    gzip=index_paths[0].endswith('.gz') if index_paths else False,
                )
            )
            for i in range(num_consumers)
        ]

        await lister_task
        logger.info('Lister completed, waiting for fetchers to finish')

        await asyncio.gather(*fetchers)
        logger.info('All fetchers completed')

        # Send stop signals to consumers
        for _ in range(num_consumers):
            await item_queue.put(_STOP)

        consumer_results = await asyncio.gather(*consumers)
        n_records = sum([result['stats']['total_requests'] for result in consumer_results])

        logger.info('All consumers completed')

    return n_records


async def get_range_jobs_from_index_paths(
    key_queue: asyncio.Queue,
    index_paths: List[str],
    warc_download_prefix: str,
    num_fetchers: int,
    limit: int = 0,
):
    """Stage 1: stream the CDX paths, parse lines -> RangeJob (WARC files and offets) -> key_queue."""

    logger.info('Range index limit: %i', limit)
    count = 0

    if not index_paths:
        logger.error('No index paths provided!')

    else:
        # Iterate over index files
        for index_path in index_paths:
            # Fetch range queries from index
            try:
                for warc_url, offset, length in iter_cdx_index_from_path(
                    index_path, warc_download_prefix=warc_download_prefix
                ):
                    # Convert the CDX record back to a RangeJob
                    bucket, key = parse_s3_uri(warc_url)
                    job = RangeJob(bucket=bucket, key=key, offset=offset, length=length)
                    await key_queue.put(job)
                    count += 1

                    if limit > 0 and count >= limit:
                        logger.warning('Index limit reached at %i', count)
                        break

            except Exception as e:
                logger.error('Failed to read CDX index from %s: %s', index_path, e)

            if limit > 0 and count >= limit:
                logger.warning('Limit reached at %i', count)
                break

    # signal fetchers to stop
    for _ in range(num_fetchers):
        await key_queue.put(_STOP)

    logger.info('Lister enqueued %d jobs from %s', count, index_path)


async def fetch_warc_ranges(
    fetcher_id: int,
    key_queue: asyncio.Queue,
    item_queue: asyncio.Queue,
    s3,
    max_attempts: int,
    base_backoff_seconds: float,
    log_every_n: int = 1000,
):
    """Stage 2: ranged GET per job -> enqueue RangePayload."""
    tracker = ThroughputTracker()
    tracker.start()
    counter = 0

    while True:
        job = await key_queue.get()
        try:
            if job is _STOP:
                stats = tracker.get_stats()
                logger.info(
                    'Fetcher %d stopping. Stats: %.1fs, %d requests, %.1f MB, %.2f MB/s, %.2f req/s',
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
                s3,
                job.bucket,
                job.key,
                job.offset,
                job.length,
                max_attempts,
                base_backoff_seconds,
            )
            tracker.add_bytes(len(data))
            counter += 1

            # Log progress every 10 items
            if counter % log_every_n == 0:
                stats = tracker.get_stats()
                logger.info(
                    'Fetcher %d: %d items, %.1f MB, %.2f MB/s, %.2f req/s',
                    fetcher_id,
                    counter,
                    stats['total_bytes'] / (1024 * 1024),
                    stats['mb_per_sec'],
                    stats['requests_per_sec'],
                )

            await item_queue.put(RangePayload(job=job, data=data))
        except Exception:
            logger.exception(
                'Fetcher %d failed on %s/%s [%d,%d]',
                fetcher_id,
                getattr(job, 'bucket', '?'),
                getattr(job, 'key', '?'),
                getattr(job, 'offset', -1),
                getattr(job, 'length', -1),
            )
        finally:
            key_queue.task_done()


def generate_warc_filename(
    dest_prefix: str,
    consumer_id: int,
    sequence: int,
    writer_subprefix: Optional[str] = None,
    gzip: bool = False,
) -> str:
    file_name = dest_prefix + '-'
    if writer_subprefix is not None:
        file_name += writer_subprefix + '-'
    file_name += '{:06d}-{:03d}'.format(consumer_id, sequence) + '.extracted.warc'
    if gzip:
        file_name += '.gz'

    return file_name


async def create_new_writer_with_header(
    s3,
    consumer_id: int,
    sequence: int,
    dest_bucket: str,
    dest_prefix: str,
    max_attempts: int,
    base_backoff_seconds: float,
    min_part_size: int,
    writer_info: Dict,
    warc_version: str = '1.0',
    writer_subprefix: Optional[str] = None,
    gzip: bool = False,
    content_type: Optional[str] = None,
):
    filename = generate_warc_filename(
        dest_prefix=dest_prefix,
        consumer_id=consumer_id,
        sequence=sequence,
        writer_subprefix=writer_subprefix,
        gzip=gzip,
    )

    new_writer = ShardWriter(
        filename,
        dest_bucket,
        content_type,
        min_part_size,
        max_attempts,
        base_backoff_seconds,
    )

    # Initialize writer
    await new_writer.start(s3)

    # Write WARC header
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warcinfo = warc_writer.create_warcinfo_record(filename, writer_info)
    warc_writer.write_record(warcinfo)
    header_data = buffer.getvalue()
    await new_writer.write(s3, header_data)

    return new_writer, len(header_data)


async def write_warc(
    consumer_id: int,
    item_queue: asyncio.Queue,
    s3,
    max_attempts: int,
    base_backoff_seconds: float,
    prefix_path: str,
    writer_info: Dict,
    writer_subprefix: Optional[str] = None,
    write_paths_as_resource_records: Optional[List[str]] = None,
    write_paths_as_resource_records_metadata: Optional[List[str]] = None,
    writer_kwargs: Optional[Dict] = None,
    warc_version: str = '1.0',
    log_every_n: int = 1000,
    gzip: bool = False,
    content_type=None,
    min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
    max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
):
    """Stage 3: Write WARC. Each consumer owns ONE shard MPU and appends ranges to it."""

    dest_bucket, dest_prefix = parse_s3_uri(prefix_path)

    # File rotation tracking
    current_file_sequence = 1
    current_file_size = 0

    # Initialize first writer with header
    writer, header_size = await create_new_writer_with_header(
        s3,
        consumer_id=consumer_id,
        sequence=current_file_sequence,
        dest_bucket=dest_bucket,
        dest_prefix=dest_prefix,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
        writer_info=writer_info,
        warc_version=warc_version,
        writer_subprefix=writer_subprefix,
        gzip=gzip,
        content_type=content_type,
        min_part_size=min_part_size,
    )
    current_file_size = header_size

    tracker = ThroughputTracker()
    tracker.start()
    counter = 0

    # Write WARC resource records
    if write_paths_as_resource_records:
        logger.info(f'Writing {len(write_paths_as_resource_records)} resource records to WARC ... ')

        # Resource records are written at the beginning the WARC file.
        for i, resource_record_path in enumerate(write_paths_as_resource_records):
            logger.info(f'Writing resource record from {resource_record_path} ...')
            resource_record = get_resource_record_from_path(
                file_path=resource_record_path,
                metadata_path=(
                    write_paths_as_resource_records_metadata[i] if write_paths_as_resource_records_metadata else None
                ),
            )
            record_data = get_bytes_from_warc_record(resource_record, warc_version=warc_version, gzip=gzip)

            await writer.write(s3, record_data)

            # Keep track but do not rotate resource records
            current_file_size += len(record_data)

        logger.info(f'Resource records added: {len(write_paths_as_resource_records)}')

    try:
        while True:
            item = await item_queue.get()
            counter += 1
            try:
                if item is _STOP:
                    stats = tracker.get_stats()
                    logger.info(
                        'Consumer %d stopping. Stats: %.1fs, %d items, %.1f MB written, %.2f MB/s write speed',
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
                    if max_file_size and current_file_size + len(item.data) > max_file_size:
                        await writer.close(s3)
                        current_file_sequence += 1

                        writer, header_size = await create_new_writer_with_header(
                            s3,
                            consumer_id=consumer_id,
                            sequence=current_file_sequence,
                            dest_bucket=dest_bucket,
                            dest_prefix=dest_prefix,
                            max_attempts=max_attempts,
                            base_backoff_seconds=base_backoff_seconds,
                            writer_info=writer_info,
                            warc_version=warc_version,
                            writer_subprefix=writer_subprefix,
                            gzip=gzip,
                            content_type=content_type,
                            min_part_size=min_part_size,
                        )

                        current_file_size = header_size
                        logger.info(f'Rotated to new WARC file sequence {current_file_sequence} due to size limit')

                    await writer.write(s3, item.data)
                    current_file_size += len(item.data)
                    tracker.add_bytes(len(item.data))

                    # Log progress every 10 items
                    if counter % log_every_n == 0:
                        stats = tracker.get_stats()
                        logger.info(
                            'Consumer %d: %d items, %.1f MB written, %.2f MB/s',
                            consumer_id,
                            counter,
                            stats['total_bytes'] / (1024 * 1024),
                            stats['mb_per_sec'],
                        )
            except Exception:
                logger.exception('Consumer %d failed on %s', consumer_id, getattr(item, 'job', None))
                should_stop = False
            finally:
                item_queue.task_done()

            if should_stop:
                break
    finally:
        await writer.close(s3)

    return {'consumer_id': consumer_id, 'stats': tracker.get_stats()}
