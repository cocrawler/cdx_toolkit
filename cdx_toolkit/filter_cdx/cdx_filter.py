import logging

from url_is_in import SURTMatcher
import fsspec

from multiprocessing import Pool
from typing import List, Tuple


logger = logging.getLogger(__name__)


def _filter_single_cdx_file(
    input_path: str,
    output_path: str,
    matcher: SURTMatcher,
    limit: int = 0,
    log_every_n: int = 100_000,
) -> Tuple[str, str, int, int, int]:
    """Process a single input/output file pair. Returns (lines_n, included_n)."""
    lines_n = 0
    included_n = 0
    errors_n = 0

    logger.info('Reading index from %s', input_path)
    logger.info('Writing filter output to %s', output_path)

    try:

        # Input/output from local or remote file system
        input_fs, input_fs_path = fsspec.url_to_fs(input_path)
        output_fs, output_fs_path = fsspec.url_to_fs(output_path)

        # Make sure output directory exists
        output_fs.makedirs(output_fs._parent(output_fs_path), exist_ok=True)

        # Read and write compressed file if needed
        compression = 'gzip' if input_fs_path.endswith('.gz') else None

        with output_fs.open(output_fs_path, 'w', compression=compression) as output_f:
            with input_fs.open(input_fs_path, 'rt', compression=compression) as input_f:
                for i, line in enumerate(input_f, 1):
                    try:
                        # Read CDX line
                        surt_length = line.find(' ')  # we do not need to parse the full line
                        record_surt = line[:surt_length]
                        lines_n += 1

                        # Use SURT matcher
                        include_record = matcher.is_in(record_surt)

                        if include_record:
                            output_f.write(line)
                            included_n += 1

                            if limit > 0 and included_n >= limit:
                                logger.info('Limit reached at %i from %s', limit, input_path)
                                break

                        if (i % log_every_n) == 0:
                            logger.info(f'Lines completed: {i:,} (matched: {included_n:,}) from {input_path}')

                    except Exception as e:
                        logger.error(f"Line processing error: {e}")
                        errors_n += 1

        # Delete file if empty
        if included_n == 0:
            logger.warning('Output file is empty, removing it: %s', output_fs_path)
            output_fs.rm(output_fs_path)

    except Exception as e:
        logger.error(f"File processing error: {e}")
        errors_n += 1

    return input_path, output_path, lines_n, included_n, errors_n


def _filter_single_cdx_file_args(kwargs: dict) -> Tuple[str, str, int, int, int]:
    """Wrapper function to unpack arguments for multiprocessing."""

    return _filter_single_cdx_file(**kwargs)


def filter_cdx(
    matcher: SURTMatcher,
    input_paths: List[str],
    output_paths: List[str],
    n_parallel: int = 1,
    limit: int = 0,
    total_lines_n: int = 0,
    total_included_n: int = 0,
    total_errors_n: int = 0,
    log_every_n: int = 100_000,
) -> Tuple[int, int, int]:
    """Filter CDX files from input paths using a matcher to output paths."""

    # Parallel processing
    logger.info('Filtering with %i processes in parallel (limit: %i)', n_parallel, limit)

    # Prepare arguments for each task (input_path, output_path, matcher, limit)
    task_args = [dict(
                    input_path=input_path, 
                    output_path=output_path, matcher=matcher, limit=limit, log_every_n=log_every_n)
                 for input_path, output_path in zip(input_paths, output_paths)]

    pool = None
    try:
        pool = Pool(processes=n_parallel)
        # Use imap for better interrupt handling
        for input_path, _, lines_n, included_n, errors_n in pool.imap(_filter_single_cdx_file_args, task_args):
            total_lines_n += lines_n
            total_included_n += included_n
            total_errors_n += errors_n

            logger.info(f'File statistics: included {total_included_n} / {total_lines_n} lines: {input_path}')

    except KeyboardInterrupt:
        logger.warning('Process interrupted by user (Ctrl+C). Terminating running tasks...')
        if pool:
            pool.terminate()
            pool.join()
        logger.info('All tasks terminated.')
    except Exception as exc:
        logger.error(f'Error during parallel processing: {exc}')
        total_errors_n += 1
    finally:
        if pool:
            pool.close()
            pool.join()

    logger.warning(f"Filter CDX errors: {total_errors_n}")

    return total_lines_n, total_included_n, total_errors_n