import math
from operator import attrgetter
import logging
import sys
import os
import os.path
import json

from pyathena import connect
import pyathena.error

LOGGER = logging.getLogger(__name__)


def all_results_properties(cursor):
    properties = ('database', 'query_id', 'query', 'statement_type',
                  'state', 'state_change_reason', 'completion_date_time',
                  'submission_date_time', 'data_scanned_in_bytes',
                  'execution_time_in_millis', 'output_location',
                  'encryption_option', 'kms_key', 'work_group')
    return dict([(p, attrgetter(p)(cursor)) for p in properties])


def estimate_athena_cost(cursor, cost_per_tib=5.0):
    data_scanned_in_bytes = cursor.data_scanned_in_bytes or 0
    data_scanned_in_mibytes = math.ceil(data_scanned_in_bytes / 1_000_000)
    return max(data_scanned_in_mibytes, 10) * cost_per_tib / 1_000_000


def print_text_messages(connection, location):
    if location is None or not location.endswith('.txt'):
        return []

    parts = location.split('/')
    bucket = parts[2]
    key = '/'.join(parts[3:])
    LOGGER.info('looking for text messages in bucket {} key {}'.format(bucket, key))

    s3 = connection.session.client('s3')  # reuse the connection parameters we already set up
    response = s3.get_object(Bucket=bucket, Key=key)
    messages = response['Body'].read().decode().splitlines()
    for m in messages:
        LOGGER.info(m)
    if messages:
        return True


database_name = 'ccindex'
table_name = 'ccindex'

# XXX ccindex -> "ccindex"."ccindex"
create_table = '''
CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (
  url_surtkey                   STRING,
  url                           STRING,
  url_host_name                 STRING,
  url_host_tld                  STRING,
  url_host_2nd_last_part        STRING,
  url_host_3rd_last_part        STRING,
  url_host_4th_last_part        STRING,
  url_host_5th_last_part        STRING,
  url_host_registry_suffix      STRING,
  url_host_registered_domain    STRING,
  url_host_private_suffix       STRING,
  url_host_private_domain       STRING,
  url_protocol                  STRING,
  url_port                      INT,
  url_path                      STRING,
  url_query                     STRING,
  fetch_time                    TIMESTAMP,
  fetch_status                  SMALLINT,
  content_digest                STRING,
  content_mime_type             STRING,
  content_mime_detected         STRING,
  content_charset               STRING,
  content_languages             STRING,
  warc_filename                 STRING,
  warc_record_offset            INT,
  warc_record_length            INT,
  warc_segment                  STRING)
PARTITIONED BY (
  crawl                         STRING,
  subset                        STRING)
STORED AS parquet
LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';
'''


find_captures = '''
SELECT url, warc_filename, warc_record_offset, warc_record_length
FROM "ccindex"."ccindex"
WHERE crawl = '%(INDEX)s'
  AND subset = '%(SUBSET)s'
  AND regexp_like(url_path, '(job|career|employ|openings|opportunities)')
  AND url_host_registered_domain = 'museums.ca'
'''
# alternately WHERE (crawl = 'x' OR crawl = 'y')
# apparently WHERE contains(ARRAY ['CC-MAIN-2019-04', 'CC-MAIN-2019-09', 'CC-MAIN-=2019-13'], crawl) works but will read the entire row (bug mentioned in amazon docs)
# LIMIT 100

find_captures_params = {
    'INDEX': 'CC-MAIN-2018-43',
    'SUBSET': 'warc',
}

jobs_surt = '''
SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE (crawl = 'CC-MAIN-2019-51')
  AND subset = 'warc'
  AND regexp_like(url_path, '(job|career|employ|openings|opportunities)')
  AND url_host_registered_domain = 'museums.ca'
  AND url_surtkey LIKE 'ca,museums)%'
'''

jobs_surt_only = '''
SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE (crawl = 'CC-MAIN-2019-51')
  AND subset = 'warc'
  AND regexp_like(url_path, '(job|career|employ|openings|opportunities)')
  AND url_surtkey LIKE 'ca,museums)%'
'''

'''
See https://github.com/commoncrawl/cc-index-table#query-the-table-in-aws-athena for more SQL examples
'''

'''
See https://github.com/commoncrawl/cc-index-table/blob/master/src/main/java/org/commoncrawl/spark/examples/CCIndexWarcExport.java
for an example of extracting the subset of warcs based on a sql query

conf.set("warc.export.description", "Common Crawl WARC export from " + tablePath + " for query: " + sqlQuery);
'''

'''
since pyathena is using boto3, then the two keys will come
from (in order) ~/.aws/credentials, ~/.aws/config, /etc/boto.cfg, ~/.boto
region_name ought to match where the cc data is (us-east-1)
region_name's name is different from the usual name region (?)
need a way to specify an alternate profile: AWS_PROFILE env or profile_name when creating a session
s3_staging_dir really ought to be in the same region as cc data (us-east-1)

can read back configured variables?
dev_s3_client = session.client('s3')
'''


def print_debug_info(params=None):
    print('here are some debugging hints: add at least one -v early in the command line', file=sys.stderr)
    if params:
        print('params:', file=sys.stderr)
        for k, v in params:
            print(' ', k, v, file=sys.stderr)
    for k, v in os.environ.items():
        if k.startswith('AWS_'):
            print(k, v, file=sys.stderr)
    for f in ('~/.aws/credentials', '~/.aws/config', '/etc/boto.cfg', '~/.boto'):
        if os.path.exists(os.path.expanduser(f)):
            print(f, 'exists', file=sys.stderr)


def get_athena(**kwargs):
    LOGGER.info('connecting to athena')

    try:
        connection = connect(**kwargs)
    except Exception:
        print_debug_info(params=kwargs)
        raise

    return connection


def asetup(connection, **kwargs):

    LOGGER.info('creating database')
    create_database = 'CREATE DATABASE ccindex'
    try:
        cursor = my_execute(connection, create_database, warn_for_cost=True, **kwargs)
    except pyathena.error.OperationalError as e:
        if 'Database ccindex already exists' in str(e):
            LOGGER.info('database ccindex already exists')
        else:
            cursor = connection.cursor()
            print_text_messages(connection, cursor.output_location)
            raise

    LOGGER.info('creating table')
    my_execute(connection, create_table, warn_for_cost=True, **kwargs)

    LOGGER.info('repairing table')
    # ccindex -> "ccindex"."ccindex"
    repair_table = '''
MSCK REPAIR TABLE ccindex;
    '''
    my_execute(connection, repair_table, warn_for_cost=True, **kwargs)


class WrapCursor:
    '''
    Make the cursor iterator easier to use, returns a dict with keys for the field names
    XXX consider making this a subclass of pyathena.cursor.Cursor ?
    '''
    def __init__(self, cursor):
        self.cursor = cursor
        self.fields = [d[0] for d in cursor.description]
        if self.fields:
            LOGGER.info('observed fields of %s', ','.join(self.fields))

    def __next__(self):
        row = next(self.cursor)
        return dict(zip(self.fields, row))

    def __iter__(self):
        return self


def my_execute(connection, sql, params={}, dry_run=False,
               print_cost=True, print_messages=True,
               warn_for_cost=False, raise_for_messages=False):

    try:
        sql = sql % params
    except KeyError as e:
        raise KeyError('sql template referenced an unknown parameter: '+str(e))
    except ValueError as e:
        if 'unsupported format character' in str(e):
            raise ValueError('did you forget to quote a percent sign?: '+str(e))

    if dry_run:
        print('final sql is:', file=sys.stderr)
        print(sql, file=sys.stderr)
        return []  # callers expect an iterable

    cursor = connection.cursor()

    try:
        cursor.execute(sql)
    except Exception:
        print_debug_info()
        raise

    m = None
    if print_messages or raise_for_messages:
        m = print_text_messages(connection, cursor.output_location)
    if print_cost or warn_for_cost:
        c = estimate_athena_cost(cursor)
        if warn_for_cost and c > 0.009:
            LOGGER.warn('estimated cost $%.6f', c)
        elif print_cost:
            LOGGER.info('estimated cost $%.6f', c)

    if m and raise_for_messages:
        raise ValueError('Expected no messages')

    return WrapCursor(cursor)


def iter(connection, **kwargs):
    # form the query:
    # all verbatim queries to be passed in
    # if not verbatim:
    #   fields -- from kwargs[fields] -- or all_fields
    #   LIMIT NNN -- from kwargs[limit]
    #   crawls -- from kwargs[crawl] []
    #   SQL WHERE clauses -- from kwargs[filter] plus url, needs translation

    LOGGER.info('executing the iter command')
    return my_execute(connection, jobs_surt, **kwargs)


def get_all_crawls(connection, **kwargs):
    get_all_crawls = '''
    SELECT DISTINCT crawl
    FROM "ccindex"."ccindex";'''

    LOGGER.info('executing get_all_crawls')

    cursor = my_execute(connection, get_all_crawls,
                        warn_for_cost=True, raise_for_messages=True, **kwargs)

    ret = []
    for row in cursor:
        ret.append(row['crawl'])

    return sorted(ret)


def get_summary(connection, **kwargs):
    count_by_partition = '''
SELECT COUNT(*) as n_captures,
       crawl,
       subset
FROM "ccindex"."ccindex"
GROUP BY crawl, subset;
'''
    LOGGER.info('executing get_summary')

    cursor = my_execute(connection, count_by_partition,
                        warn_for_cost=True, raise_for_messages=True, **kwargs)

    return '\n'.join([json.dumps(row, sort_keys=True) for row in cursor])


def run_sql_from_file(connection, cmd, **kwargs):
    with open(cmd.file, 'r') as fd:
        sql = fd.read()

    params = {}
    if cmd.param:
        for p in cmd.param:
            if '=' not in p:
                raise ValueError('paramters should have a single equals sign')
            k, v = p.split('=', 1)
            params[k] = v

    LOGGER.info('executing sql from file %s', cmd.file)
    cursor = my_execute(connection, sql, params=params, **kwargs)

    return '\n'.join([json.dumps(row, sort_keys=True) for row in cursor])


#if __name__ == '__main__':
#    s3_staging_dir = 's3://dshfjhfkjhdfshjgdghj/staging/'  # AWS_ATHENA_S3_STAGING_DIR
#    ga_kwargs = dict(profile_name='greg',  # AWS_PROFILE
#                     schema_name='ccindex',  # needed for a couple of actions that don't mention the database
#                     s3_staging_dir=s3_staging_dir,  # AWS_ATHENA_S3_STAGING_DIR optional if work_group is set
#                     region_name='us-east-1')  # AWS_DEFAULT_REGION
#    connection = get_athena(**ga_kwargs)
#
#    print(get_all_crawls(connection))
