import asyncio
from io import BytesIO
import logging
from typing import List

import aioboto3
from botocore.config import Config
from warcio import WARCWriter

from cdx_toolkit.warcer_by_cdx.aioboto3_utils import (
    _STOP,
    RangeJob,
    RangePayload,
    ThroughputTracker,
    parse_s3_uri,
    ranged_get_bytes,
)
from cdx_toolkit.warcer_by_cdx.aioboto3_writer import ShardWriter
from cdx_toolkit.warcer_by_cdx.cdx_utils import (
    read_cdx_index_from_s3,
)


logger = logging.getLogger(__name__)


def filter_warc_by_cdx_via_aioboto3(
    index_paths: List[str],
    prefix_path: str,
    writer_info: dict,
    writer_subprefix: str | None = None,
    write_index_as_record: bool = False,
    limit: int = 0,
    log_every_n: int = 1000,
    warc_download_prefix: str | None = None,
    n_parallel: int = 1,
    writer_kwargs: dict | None = None,
) -> int:
    try:
        return asyncio.run(
            filter_warc_by_cdx_via_aioboto3_async(
                index_paths=index_paths,
                prefix_path=prefix_path,
                writer_info=writer_info,
                writer_subprefix=writer_subprefix,
                write_index_as_record=write_index_as_record,
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
    writer_info: dict,
    writer_subprefix: str | None = None,
    write_index_as_record: bool = False,
    limit: int = 0,
    log_every_n: int = 1000,
    warc_download_prefix: str | None = None,
    n_parallel: int = 1,
    writer_kwargs: dict | None = None,
    max_attempts: int = 5,
    key_queue_size: int = 1000,
    item_queue_size: int = 200,
    base_backoff_seconds=0.5,
) -> int:
    n_records = 0
    fetcher_to_consumer_ratio = 6
    num_fetchers = n_parallel
    num_consumers = max(int(num_fetchers / fetcher_to_consumer_ratio), 1)

    key_queue: asyncio.Queue = asyncio.Queue(maxsize=key_queue_size)
    item_queue: asyncio.Queue = asyncio.Queue(maxsize=item_queue_size)

    boto_cfg = Config(
        region_name='us-east-1',
        retries={'max_attempts': max(2, max_attempts), 'mode': 'standard'},
        connect_timeout=10,
        read_timeout=120,
    )

    if write_index_as_record:
        raise NotImplementedError

    session = aioboto3.Session()

    async with session.client('s3', config=boto_cfg) as s3:
        # Fetch file paths and ranges (offset, length) from index files
        logger.info('Starting lister, %d fetchers, %d consumers', num_fetchers, num_consumers)
        lister_task = asyncio.create_task(
            lister_from_index(
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
                fetcher(
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
                consumer(
                    consumer_id=i,
                    item_queue=item_queue,
                    s3=s3,
                    prefix_path=prefix_path,
                    max_attempts=max_attempts,
                    base_backoff_seconds=base_backoff_seconds,
                    write_index_as_record=write_index_as_record,
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


async def lister_from_index(
    key_queue: asyncio.Queue,
    index_paths: List[str],
    warc_download_prefix: str,
    num_fetchers: int,
    limit: int = 0,
):
    """Stage 1: stream the index, parse lines -> RangeJob -> key_queue."""

    logger.info('Range index limit: %i', limit)
    count = 0

    if not index_paths:
        logger.error('No index paths provided!')

    else:
        # Iterate over index files
        for index_path in index_paths:
            # Fetch range queries from index
            try:
                for warc_url, offset, length in read_cdx_index_from_s3(
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


async def fetcher(
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


async def consumer(
    consumer_id: int,
    item_queue: asyncio.Queue,
    s3,
    # shard_name_prefix: str,
    # shard_extension: str,
    # dest_prefix: str,
    # dest_bucket: str,
    # content_type: str | None,
    # min_part_size: int,
    max_attempts: int,
    base_backoff_seconds: float,
    prefix_path: str,
    writer_info: dict,
    writer_subprefix: str | None = None,
    write_index_as_record: bool = False,
    writer_kwargs: dict | None = None,
    warc_version: str = '1.0',
    log_every_n: int = 1000,
    gzip: bool = False,
):
    """Stage 3: each consumer owns ONE shard MPU and appends ranges to it."""

    dest_bucket, dest_prefix = parse_s3_uri(prefix_path)

    min_part_size = 5 * 1024 * 1024  # 5 MiB
    content_type = None

    file_name = dest_prefix + '-'
    if writer_subprefix is not None:
        file_name += writer_subprefix + '-'
    file_name += '{:06d}'.format(consumer_id) + '.extracted.warc'

    if gzip:
        file_name += '.gz'

    writer = ShardWriter(
        file_name,
        dest_bucket,
        content_type,
        min_part_size,
        max_attempts,
        base_backoff_seconds,
    )
    tracker = ThroughputTracker()
    tracker.start()
    counter = 0

    # Initialize writer
    await writer.start(s3)

    # Write WARC header
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warcinfo = warc_writer.create_warcinfo_record(file_name, writer_info)
    warc_writer.write_record(warcinfo)

    await writer.write(s3, buffer.getvalue())

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
                    await writer.write(s3, item.data)
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
