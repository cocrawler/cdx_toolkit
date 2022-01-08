from argparse import ArgumentParser
import logging
import csv
import sys
import json
import os

import cdx_toolkit
from . import athena

LOGGER = logging.getLogger(__name__)


def split_fields(fields):
    ret = []
    dedup = set()
    for f in fields.split(','):
        if f not in dedup:
            dedup.add(f)
            ret.append(f)
    return ret


def add_global_args(parser):
    parser.add_argument('--version', '-V', action='version', version=get_version())
    parser.add_argument('--verbose', '-v', action='count', help='set logging level to INFO (-v) or DEBUG (-vv)')
    parser.add_argument('--limit', type=int, action='store')
    parser.add_argument('--from', action='store')  # XXX default for cc
    parser.add_argument('--to', action='store')
    parser.add_argument('--filter', action='append', help='see CDX API documentation for usage')


def main(args=None):
    parser = ArgumentParser(description='cdx_toolkit iterator command line tool')

    add_global_args(parser)

    parser.add_argument('--cc', action='store_const', const='cc', help='direct the query to the Common Crawl CDX/WARCs')
    parser.add_argument('--ia', action='store_const', const='ia', help='direct the query to the Internet Archive CDX/wayback')
    parser.add_argument('--source', action='store', help='direct the query to this CDX server')
    parser.add_argument('--wb', action='store', help='direct replays for content to this wayback')
    parser.add_argument('--cc-mirror', action='store', help='use this Common Crawl index mirror')
    parser.add_argument('--cc-sort', action='store', help='default mixed, alternatively: ascending')
    parser.add_argument('--get', action='store_true', help='use a single get instead of a paged iteration. default limit=1000')
    parser.add_argument('--closest', action='store', help='get the closest capture to this timestamp. use with --get')

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    iterate = subparsers.add_parser('iter', help='iterate printing captures')
    iterate.add_argument('--all-fields', action='store_true')
    iterate.add_argument('--fields', action='store', default='url,status,timestamp', help='try --all-fields if you need the complete list')
    iterate.add_argument('--jsonl', action='store_true')
    iterate.add_argument('--csv', action='store_true')
    iterate.add_argument('url')
    iterate.set_defaults(func=iterator)

    warc = subparsers.add_parser('warc', help='iterate over capture content, creating a warc')
    warc.add_argument('--prefix', default='TEST', help='prefix for the warc filename')
    warc.add_argument('--subprefix', type=str, default=None, help='subprefix for the warc filename, default None')
    warc.add_argument('--size', type=int, default=1000000000, help='target for the warc filesize in bytes')
    warc.add_argument('--creator', action='store', help='creator of the warc: person, organization, service')
    warc.add_argument('--operator', action='store', help='a person, if the creator is an organization')
    warc.add_argument('--url-fgrep', action='store', help='this pattern must be present to warc an url')
    warc.add_argument('--url-fgrepv', action='store', help='this pattern must not be present to warc an url, e.g. /robots.txt')
    warc.add_argument('url')
    warc.set_defaults(func=warcer)

    size = subparsers.add_parser('size', help='imprecise count of how many results are available')
    size.add_argument('--details', action='store_true', help='show details of each subindex')
    size.add_argument('url', help='')
    size.set_defaults(func=sizer)

    if args is not None:
        cmdline = ' '.join(args)
    else:  # pragma: no cover
        # there's something magic about args and console_scripts
        # this fallback is needed when installed by setuptools
        if len(sys.argv) > 1:
            cmdline = 'cdxt ' + ' '.join(sys.argv[1:])
        else:
            cmdline = 'cdxt'
    cmd = parser.parse_args(args=args)
    set_loglevel(cmd)
    cmd.func(cmd, cmdline)


def add_athena_args(parser):
    parser.add_argument('--profile-name', action='store', help='choose which section of your boto conf files is used')
    parser.add_argument('--role-arn', action='store', help='Amazon resource name roley')
    parser.add_argument('--work-group', action='store', help='Amazon Athena work group name')
    parser.add_argument('--s3-staging-dir', action='store', help='an s3 bucket to hold outputs')
    parser.add_argument('--region-name', action='store', default='us-east-1',
                        help='AWS region to use, you probably want the one the commoncrawl data is in (us-east-1)')
    parser.add_argument('--dry-run', '-n', action='store_true', help='print the SQL and exit without executing it')


def main_athena(args=None):
    parser = ArgumentParser(description='CommonCrawl column database command line tool')

    add_global_args(parser)
    add_athena_args(parser)

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    asetup = subparsers.add_parser('setup', help='set up amazon athena ccindex database and table')
    asetup.set_defaults(func=asetuper)

    asummarize = subparsers.add_parser('summarize', help='summarize the partitions currently in the table')
    asummarize.set_defaults(func=asummarizer)

    asql = subparsers.add_parser('sql', help='run arbitrary SQL statement from a file')
    asql.add_argument('--param', action='append', help='parameteres for templating the SQL, e.g. SUBSET=warc')
    asql.add_argument('file', help='')
    asql.set_defaults(func=asqler)

    aiter = subparsers.add_parser('iter', help='iterate printing captures')
    aiter.add_argument('--all-fields', action='store_true')
    aiter.add_argument('--fields', action='store', default='url,warc_filename,warc_record_offset,warc_record_length', help='try --all-fields if you need the list')
    aiter.add_argument('--jsonl', action='store_true')
    aiter.add_argument('--csv', action='store_true')
    aiter.add_argument('--subset', action='append', default='warc', help='e.g. warc, robotstxt, crawldiagnostics')
    aiter.add_argument('--crawl', action='append', help='crawl to process, you can specify more than one')
    aiter.add_argument('--limit', type=int, action='store', help='maximum records to return, good for debugging')
    aiter.add_argument('--filter', action='append', help='CDX-style filter, see CDX API documentation for usage')
    aiter.add_argument('url', help='')
    aiter.set_defaults(func=aiterator)

    if args is not None:
        cmdline = ' '.join(args)
    else:  # pragma: no cover
        # there's something magic about args and console_scripts
        # this fallback is needed when installed by setuptools
        if len(sys.argv) > 1:
            cmdline = 'cdxt ' + ' '.join(sys.argv[1:])
        else:
            cmdline = 'cdxt'
    cmd = parser.parse_args(args=args)
    set_loglevel(cmd)
    cmd.func(cmd, cmdline)


def set_loglevel(cmd):
    loglevel = os.getenv('LOGLEVEL') or 'WARNING'
    if cmd.verbose:
        if cmd.verbose > 0:
            loglevel = 'INFO'
        if cmd.verbose > 1:
            loglevel = 'DEBUG'

    # because pytest has already initialized the logger, we have to set the root logger
    logging.getLogger().setLevel(loglevel)
    # for the normal case
    logging.basicConfig(level=loglevel)

    LOGGER.info('set loglevel to %s', str(loglevel))


def get_version():
    return cdx_toolkit.__version__


def setup(cmd):
    kwargs = {}
    kwargs['source'] = cmd.cc or cmd.ia or cmd.source or None
    if kwargs['source'] is None:
        raise ValueError('must specify --cc, --ia, or a --source')
    if cmd.wb:
        kwargs['wb'] = cmd.wb
    if cmd.cc_mirror:
        kwargs['cc_mirror'] = cmd.cc_mirror

    cdx = cdx_toolkit.CDXFetcher(**kwargs)

    kwargs = {}
    if cmd.limit:
        kwargs['limit'] = cmd.limit
    if 'from' in vars(cmd) and vars(cmd)['from']:  # python, uh, from is a reserved word
        kwargs['from_ts'] = vars(cmd)['from']
    if cmd.to:
        kwargs['to'] = cmd.to
    if cmd.closest:
        if not cmd.get:  # pragma: no cover
            LOGGER.info('note: --closest works best with --get')
        kwargs['closest'] = cmd.closest
    if cmd.filter:
        kwargs['filter'] = cmd.filter

    if cmd.cmd == 'warc' and cmd.size:
        kwargs['size'] = cmd.size

    if cmd.cmd == 'size' and cmd.details:
        kwargs['details'] = cmd.details

    return cdx, kwargs


def winnow_fields(cmd, fields, obj):
    if cmd.all_fields:
        printme = obj
    else:
        printme = dict([(k, obj[k]) for k in fields if k in obj])
    return printme


def print_line(cmd, writer, printme):
    if cmd.jsonl:
        print(json.dumps(printme, sort_keys=True))
    elif writer:
        writer.writerow(printme)
    else:
        print(', '.join([' '.join((k, printme[k])) for k in sorted(printme.keys())]))


def iterator(cmd, cmdline):
    cdx, kwargs = setup(cmd)

    fields = split_fields(cmd.fields)

    if cmd.csv and cmd.all_fields:
        raise NotImplementedError('Sorry, the comination of csv and all-fields is not yet implemented')
    if cmd.csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=fields)
        writer.writeheader()
    else:
        writer = None

    if cmd.get:
        objs = cdx.get(cmd.url, **kwargs)
        for obj in objs:
            printme = winnow_fields(cmd, fields, obj)
            print_line(cmd, writer, printme)
        return

    for obj in cdx.iter(cmd.url, **kwargs):
        printme = winnow_fields(cmd, fields, obj)
        print_line(cmd, writer, printme)


def warcer(cmd, cmdline):
    cdx, kwargs = setup(cmd)

    ispartof = cmd.prefix
    if cmd.subprefix:
        ispartof += '-' + cmd.subprefix

    info = {
        'software': 'pypi_cdx_toolkit/'+get_version(),
        'isPartOf': ispartof,
        'description': 'warc extraction generated with: '+cmdline,
        'format': 'WARC file version 1.0',  # todo: if we directly read a warc, have this match the warc
    }
    if cmd.creator:
        info['creator'] = cmd.creator
    if cmd.operator:
        info['operator'] = cmd.operator

    kwargs_writer = {}
    if 'size' in kwargs:
        kwargs_writer['size'] = kwargs['size']
        del kwargs['size']

    writer = cdx_toolkit.warc.get_writer(cmd.prefix, cmd.subprefix, info, **kwargs_writer)

    for obj in cdx.iter(cmd.url, **kwargs):
        url = obj['url']
        if cmd.url_fgrep and cmd.url_fgrep not in url:
            LOGGER.debug('not warcing due to fgrep: %s', url)
            continue
        if cmd.url_fgrepv and cmd.url_fgrepv in url:
            LOGGER.debug('not warcing due to fgrepv: %s', url)
            continue
        timestamp = obj['timestamp']
        try:
            record = obj.fetch_warc_record()
        except RuntimeError:  # pragma: no cover
            LOGGER.warning('skipping capture for RuntimeError 404: %s %s', url, timestamp)
            continue
        if obj.is_revisit():
            LOGGER.warning('revisit record being resolved for url %s %s', url, timestamp)
        writer.write_record(record)


def sizer(cmd, cmdline):
    cdx, kwargs = setup(cmd)

    size = cdx.get_size_estimate(cmd.url, **kwargs)
    print(size)


def athena_init(cmd):
    conn_kwargs = {}
    # these args are all permissions-related
    for k in ('profile_name', 'role_arn', 'work_group', 's3_staging_dir', 'region_name', 'dry_run'):
        if k in cmd:
            value = cmd.__dict__[k]
            if value is not None:
                conn_kwargs[k] = value
    kwargs = {}
    if 'dry_run' in cmd and cmd.dry_run:
        kwargs['dry_run'] = True

    connection = athena.get_athena(**conn_kwargs)
    return connection, kwargs


def asetuper(cmd, cmdline):
    connection, kwargs = athena_init(cmd)
    athena.asetup(connection, **kwargs)
    print('crawl partitions:', athena.get_all_crawls(connection, **kwargs))


def asummarizer(cmd, cmdline):
    connection, kwargs = athena_init(cmd)
    print(athena.asummarize(connection, **kwargs))


def asqler(cmd, cmdline):
    connection, kwargs = athena_init(cmd)
    print(athena.asql(connection, cmd, **kwargs))


def aiterator(cmd, cmdline):
    connection, kwargs = athena_init(cmd)

    fields = split_fields(cmd.fields)

    if cmd.csv and cmd.all_fields:
        raise NotImplementedError('Sorry, the comination of csv and all-fields is not yet implemented')
    if cmd.csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=sorted(list(fields)))
        writer.writeheader()
    else:
        writer = None

    # csv fields for all-fields are not present until the cursor.execute has run
    # XXX what should winnow_fields do in this loop?

    for obj in athena.iter(connection, **kwargs):
        printme = winnow_fields(cmd, fields, obj)
        print_line(cmd, writer, printme)
