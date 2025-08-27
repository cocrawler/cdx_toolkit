import cdx_toolkit
from cdx_toolkit.commoncrawl import normalize_crawl

import logging

LOGGER = logging.getLogger(__name__)


def get_version():
    return cdx_toolkit.__version__


def setup(cmd):
    kwargs = {}
    kwargs['source'] = 'cc' if cmd.crawl else cmd.cc or cmd.ia or cmd.source or None
    if kwargs['source'] is None:
        raise ValueError('must specify --cc, --ia, or a --source')
    if cmd.wb:
        kwargs['wb'] = cmd.wb
    if cmd.cc_mirror:
        kwargs['cc_mirror'] = cmd.cc_mirror
    if cmd.crawl:
        kwargs['crawl'] = normalize_crawl([cmd.crawl])  # currently a string, not a list
    if getattr(cmd, 'warc_download_prefix', None) is not None:
        kwargs['warc_download_prefix'] = cmd.warc_download_prefix

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
