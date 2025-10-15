from argparse import ArgumentParser
import logging
import csv
import sys
import json
import os

import cdx_toolkit

from cdx_toolkit.utils import get_version, setup

from cdx_toolkit.filter_cdx.command import run_filter_cdx
from cdx_toolkit.filter_cdx.args import add_filter_cdx_args

from cdx_toolkit.filter_warc.command import run_warcer_by_cdx
from cdx_toolkit.filter_warc.args import add_warcer_by_cdx_args


LOGGER = logging.getLogger(__name__)


def main(args=None):
    parser = ArgumentParser(description='cdx_toolkit iterator command line tool')

    parser.add_argument('--version', '-V', action='version', version=get_version())
    parser.add_argument('--verbose', '-v', action='count', help='set logging level to INFO (-v) or DEBUG (-vv)')

    parser.add_argument('--cc', action='store_const', const='cc', help='direct the query to the Common Crawl CDX/WARCs')
    parser.add_argument('--crawl', action='store', help='crawl names (comma separated) or an integer for the most recent N crawls. Implies --cc')
    parser.add_argument('--ia', action='store_const', const='ia', help='direct the query to the Internet Archive CDX/wayback')
    parser.add_argument('--source', action='store', help='direct the query to this CDX server')
    parser.add_argument('--wb', action='store', help='direct replays for content to this wayback')
    parser.add_argument('--limit', type=int, action='store')
    parser.add_argument('--cc-mirror', action='store', help='use this Common Crawl index mirror')
    parser.add_argument('--cc-sort', action='store', help='default mixed, alternatively: ascending')
    parser.add_argument('--from', action='store')
    parser.add_argument('--to', action='store')
    parser.add_argument('--filter', action='append', help='see CDX API documentation for usage')
    parser.add_argument('--get', action='store_true', help='use a single get instead of a paged iteration. default limit=1000')
    parser.add_argument('--closest', action='store', help='get the closest capture to this timestamp. use with --get')

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    iterate = subparsers.add_parser('iter', help='iterate printing captures')
    iterate.add_argument('--all-fields', action='store_true')
    iterate.add_argument('--fields', action='store', default='url,status,timestamp', help='try --all-fields if you need the list')
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
    warc.add_argument('--warc-download-prefix', action='store', help='prefix for downloading content, automatically set for CC')
    warc.add_argument('url')
    warc.set_defaults(func=warcer)

    warc_by_cdx = subparsers.add_parser('warc_by_cdx', help='iterate over capture content based on an CDX index file, creating a warc')
    add_warcer_by_cdx_args(warc_by_cdx)
    warc_by_cdx.set_defaults(func=run_warcer_by_cdx)

    filter_cdx = subparsers.add_parser('filter_cdx', help='Filter CDX files based on SURT prefixes whitelist')
    add_filter_cdx_args(filter_cdx)
    filter_cdx.set_defaults(func=run_filter_cdx)

    size = subparsers.add_parser('size', help='imprecise count of how many results are available')
    size.add_argument('--details', action='store_true', help='show details of each subindex')
    size.add_argument('url')
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
    fields = set(cmd.fields.split(','))
    if cmd.csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=sorted(list(fields)))
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

    writer.close()


def sizer(cmd, cmdline):
    cdx, kwargs = setup(cmd)

    size = cdx.get_size_estimate(cmd.url, **kwargs)
    print(size)


if __name__ == "__main__":
    main()
