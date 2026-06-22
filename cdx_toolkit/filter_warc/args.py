import logging
import argparse


logger = logging.getLogger(__name__)


def add_repackage_args(parser: argparse.ArgumentParser):
    # --- CDX source ---
    parser.add_argument(
        '--cdx-path',
        type=str,
        default=None,
        help='Path to CDX index file (local or remote, e.g. S3). Used when --target-source cdx.',
    )
    parser.add_argument(
        '--cdx-glob',
        type=str,
        default=None,
        help='a glob pattern for read from multiple CDX indices',
    )
    # --- SQL source (--target-source sql --engine athena|duckdb) ---
    parser.add_argument(
        '--engine',
        type=str,
        default=None,
        choices=['athena', 'duckdb'],
        help='SQL engine for the columnar index. Required when --target-source sql.',
    )
    parser.add_argument(
        '--hostnames',
        type=str,
        nargs='+',
        default=None,
        help=('Exact hostnames (url_host_name, e.g. www.example.com) to filter for via the SQL '
              'index. Combine with --domains; mutually exclusive with --query/--query-file. '
              'Combine with the global --crawl to restrict the scan to specific crawls '
              '(strongly recommended for cost).'),
    )
    parser.add_argument(
        '--domains',
        type=str,
        nargs='+',
        default=None,
        help=('Registered domains (url_host_registered_domain, e.g. example.com) to filter for via '
              'the SQL index; also matches subdomains. Combine with --hostnames; mutually exclusive '
              'with --query/--query-file.'),
    )
    parser.add_argument(
        '--query',
        type=str,
        default=None,
        help=('Raw SQL to run instead of the hostname-based query (power users). Must SELECT the '
              'columns warc_filename, warc_record_offset, warc_record_length. Engine-specific '
              'dialect. Mutually exclusive with --hostnames and --query-file.'),
    )
    parser.add_argument(
        '--query-file',
        type=str,
        default=None,
        help='Path to a file containing the raw SQL (alternative to --query).',
    )
    parser.add_argument(
        '--athena-database',
        type=str,
        default=None,
        help='Athena database (engine=athena). Defaults to `ccindex`.',
    )
    parser.add_argument(
        '--athena-s3-output',
        type=str,
        default=None,
        help='Athena S3 output location (engine=athena). Required for engine=athena.',
    )
    parser.add_argument(
        '--duckdb-index-path',
        type=str,
        default='s3://commoncrawl/cc-index/table/cc-main/warc/',
        help='Base S3 path to the CC columnar index parquet (engine=duckdb).',
    )
    parser.add_argument(
        '--confirm-cost',
        action='store_true',
        help=('Skip the cost-confirmation prompt and run even unpartitioned / large-scan SQL '
              'queries. Athena bills per TB scanned; restrict with --crawl to reduce cost.'),
    )
    # --- CSV source ---
    parser.add_argument(
        '--csv-path',
        type=str,
        default=None,
        help='Path to a range-jobs CSV/TSV (local or remote). Used when --target-source csv.',
    )
    # --- Range-jobs materialization (any source) ---
    parser.add_argument(
        '--range-jobs-output',
        type=str,
        default=None,
        help='If set, write each generated RangeJob to this CSV (filename,offset,length by default).',
    )
    parser.add_argument(
        '--no-fetch',
        action='store_true',
        help='Only generate range jobs (write --range-jobs-output); skip fetching/writing WARCs.',
    )
    parser.add_argument(
        '--csv-self-contained',
        action='store_true',
        help='Write full URLs (warc_url,...) to --range-jobs-output instead of relative filenames.',
    )
    parser.add_argument(
        '--no-sort-ranges',
        action='store_true',
        help=('Do not sort range jobs by (warc_filename, warc_record_offset) before fetching. '
              'Sorting (the default) groups records of the same WARC file with ascending offsets '
              'for better S3 range-read locality; it adds an ORDER BY to a guided SQL query and '
              'buffers a CSV source in memory. Disable for an already-sorted or very large CSV, or '
              'to preserve a raw query/CSV order.'),
    )
    parser.add_argument('--prefix', default='TEST', help='prefix for the output warc filename')
    parser.add_argument(
        '--subprefix',
        type=str,
        default=None,
        help='subprefix for the warc filename, default None',
    )
    parser.add_argument(
        '--size',
        type=int,
        default=1000000000,
        help='target for the warc filesize in bytes',
    )
    parser.add_argument(
        '--creator',
        action='store',
        help='creator of the warc: person, organization, service',
    )
    parser.add_argument('--operator', action='store', help='a person, if the creator is an organization')
    parser.add_argument(
        '--description',
        action='store',
        help='the `description` field in the `warcinfo` record (auto-generated if not set)',
    )
    parser.add_argument(
        '--is-part-of',
        action='store',
        help='the `isPartOf` field in the `warcinfo` record (auto-generated if not set)',
    )
    parser.add_argument(
        '--warc-download-prefix',
        action='store',
        help='prefix for downloading content, automatically set for CC. Supports '
             's3:// (fastest, in-region), https:// and hf://buckets/<ns>/<name> '
             '(Hugging Face Storage Bucket).',
        default='https://data.commoncrawl.org',
    )
    parser.add_argument(
        '--hf-reader',
        choices=['fsspec', 'cdn'],
        default='fsspec',
        help="How to read WARC ranges when --warc-download-prefix is hf:// : "
             "'fsspec' uses HfFileSystem (handles auth/Xet/CDN; default), 'cdn' uses "
             "async HTTP ranged GETs against the CDN-fronted resolve URL. Ignored for "
             "s3:// and https:// prefixes.",
    )
    parser.add_argument(
        '--write-paths-as-metadata-records',
        nargs='*',
        help='Paths to multiple files. File content is written to as a metadata record to each the WARC file',
    )
    parser.add_argument(
        '--processes',
        type=int,
        default=1,
        help=('Number of worker processes for fetching (default: 1). A single asyncio loop '
              'saturates one CPU core on many small range reads; set this to the vCPU count '
              'to use all cores. The range jobs are sharded by WARC filename across processes '
              'and the per-process output shards are merged into a single <prefix>.warc.gz '
              '(one warcinfo record). Each process uses --parallel_readers async readers.'),
    )
    parser.add_argument(
        '--keep-shards',
        action='store_true',
        help='In multi-process mode, keep the intermediate per-process shard WARCs instead of '
             'deleting them after the merge.',
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=1,
        help='Number of async readers per process for fetching WARC records (default: 1). Each '
             'process has a single writer; combine with --processes to use multiple cores.',
    )
    parser.add_argument(
        '--parallel_readers',
        type=int,
        default=None,
        help='Number of async readers per process for reading WARC records (default: same as '
             '`parallel`). Each process has a single writer; use --processes for multi-core '
             'scaling and output sharding.',
    )
    parser.add_argument(
        '--log_every_n',
        type=int,
        default=1000,
        help='Every N extracted record a log message is emitted (0 = no record logs)',
    )
    parser.add_argument(
        '--target-source',
        action='store',
        default='cdx',
        choices=['cdx', 'sql', 'csv'],
        help=('Where range jobs come from: `cdx` (index files), `sql` (columnar index via '
              '--engine athena|duckdb), or `csv` (a range-jobs CSV). Defaults to `cdx`. For `sql`, '
              'use the global --crawl to restrict the scan to specific crawls (recommended for cost).'),
    )
    return parser
