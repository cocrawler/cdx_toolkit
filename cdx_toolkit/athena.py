import math
from operator import attrgetter
import logging
import sys
import os
import os.path
import json

from pyathena import connect
import pyathena.error
from pyathena.cursor import Cursor, DictCursor
from pyathena.pandas.cursor import PandasCursor
from pyathena.async_cursor import AsyncCursor, AsyncDictCursor
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.utils import parse_output_location

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
    data_scanned_in_mibytes = math.ceil(data_scanned_in_bytes / 1000000)
    return max(data_scanned_in_mibytes, 10) * cost_per_tib / 1000000


def print_text_messages(connection, location):
    if location is None or not location.endswith('.txt'):
        return []

    bucket, key = parse_output_location(location)
    LOGGER.info('looking for text messages in bucket {} key {}'.format(bucket, key))

    s3 = connection.session.client('s3')  # reuse the connection parameters we already set up
    response = s3.get_object(Bucket=bucket, Key=key)
    messages = response['Body'].read().decode().splitlines()
    for m in messages:
        LOGGER.info(m)
    if messages:
        return True


def download_result_csv(connection, location, output_file):
    if location is None or not location.endswith('.csv'):
        raise ValueError('athena query did not return a csv')

    bucket, key = parse_output_location(location)
    LOGGER.info('looking for csv in bucket {} key {}'.format(bucket, key))
    s3 = connection.session.client('s3')  # reuse the connection parameters we already set up
    try:
        s3.Bucket(bucket).download_file(key, output_file)
    except Exception:
        raise


database_name = 'ccindex'
table_name = 'ccindex'

# depends on schema_name="ccindex'
# https://github.com/commoncrawl/cc-index-table/blob/master/src/sql/athena/cc-index-create-table-flat.sql

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
  url_host_name_reversed        STRING,
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


def debug_credentials(params=None):
    print('here are some debugging hints: add at least one -v early in the command line', file=sys.stderr)
    print('these are in the same order that boto3 uses them:', file=sys.stderr)

    if params:
        print('params:', file=sys.stderr)
        for k, v in params:
            print(' ', k, v, file=sys.stderr)
    profile_name = params.get('profile_name')

    print('environment variables', file=sys.stderr)
    for k, v in os.environ.items():
        if k.startswith('AWS_'):
            if k in {'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'AWS_SECURITY_TOKEN'}:
                v = '<secret>'
            print(k, v, file=sys.stderr)
            if k == 'AWS_SECURITY_TOKEN':
                print('AWS_SECURITY_TOKEN is deprecated', file=sys.stderr)

    if 'AWS_PROFILE' not in os.environ and not profile_name:
        print('NOTE: AWS_PROFILE is not set, so we will look for the default profile', file=sys.stderr)
        profile_name = 'default'
    elif not profile_name:
        profile_name = os.environ.get('AWS_PROFILE', 'default')

    scf = os.environ.get('AWS_SHARED_CREDENTIALS_FILE', '~/.aws/credentials')
    scf = os.path.expanduser(scf)
    if os.path.exists(scf):
        print(scf, 'exists', file=sys.stderr)
        # XXX read it
        # only allowed to have 3 keys. every section is a profile name
        # aws_access_key_id, aws_secret_access_key, aws_session_token

    if os.path.exists(os.path.expanduser('~/.aws/config')):
        print('~/.aws/config', 'exists', file=sys.stderr)
        # XXX read it
        # profiles have to have section names like "profile prod"
        # in addition to profiles with the 3 keys, this can have region, region_name, s3_staging_dir
        # can also have "assume role provider" in a profile
        # source_profile=foo will look for foo in either config or credentials

    for f in ('/etc/boto.cfg', '~/.boto'):
        if os.path.exists(os.path.expanduser(f)):
            print(f, 'exists', file=sys.stderr)
            # XXX only uses the [Credentials] section

    print('finally: instance metadata if running in EC2 with IAM', file=sys.stderr)


def log_session_info(session):
    LOGGER.debug('Session info:')
    LOGGER.debug('session.profile_name: ' + session.profile_name)
    LOGGER.debug('session.available_profiles: ' + session.available_profiles)
    LOGGER.debug('session.region_name: ' + session.region_name)
    # get_credentials ? botocore.credential.Credential


kinds = {
    'default': {'cursor': Cursor},
    'dict': {'cursor': DictCursor},
    'pandas': {'cursor': PandasCursor},
    'default_async': {'cursor': AsyncCursor},
    'dict_async': {'cursor': AsyncDictCursor},
    'pandas_async': {'cursor': AsyncPandasCursor},
}


def get_athena(**kwargs):
    LOGGER.info('connecting to athena')

    # XXX verify that s3_staging_dir is set, and not set to common crawl's bucket (which is ro)
    # XXX verify that there is a schema_name

    cursor_class = kwargs.pop('cursor_class', None)
    if not cursor_class:
        kind = kwargs.pop('kind', 'default')
        cursor_class = kinds.get(kind)
        if not cursor_class:
            raise ValueError('Unknown cursor kind of '+kind)
    elif kind in kwargs and kwargs[kind]:
        LOGGER.warning('User specified both cursor_class and kind, ignoring kind')

    try:
        connection = connect(cursor_class=cursor_class, **kwargs)
    except Exception:
        debug_credentials(params=kwargs)
        raise

    log_session_info(connection.session)

    # XXX cursor._result_set_class = OurExponentialResultClass
    return connection


def asetup(connection, **kwargs):
    LOGGER.info('creating database')

    # XXX verify that there is a schema_name? needed for create and repair

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
    # depends on schema_name="ccindex'
    repair_table = '''
MSCK REPAIR TABLE ccindex;
    '''
    my_execute(connection, repair_table, warn_for_cost=True, **kwargs)


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
        debug_credentials()
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
        raise ValueError('Expected no messages, see above')

    return cursor


def aiter(connection, **kwargs):
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


def asummary(connection, **kwargs):
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


def asql(connection, cmd, **kwargs):
    with open(cmd.file, 'r') as fd:
        sql = fd.read()

    params = {}
    if cmd.param:
        for p in cmd.param:
            if p.count('=') != 1:
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
