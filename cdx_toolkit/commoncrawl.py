'''
Code specific to accessing the Common Crawl index
'''
import time
import re
import bisect
import os
import os.path
import json
import logging

from .myrequests import myrequests_get
from .timeutils import time_to_timestamp, timestamp_to_time, pad_timestamp, pad_timestamp_up, cc_index_to_time, cc_index_to_time_special

LOGGER = logging.getLogger(__name__)


def normalize_crawl(crawl):
    crawls = []
    for c in crawl:
        if ',' in c:
            crawls.extend(c.split(','))
        else:
            crawls.append(c)
    if len(crawls) > 1 and (any(x.isdigit() for x in crawls)):
        raise ValueError('If you specify an integer, only one crawl is allowed')
    return crawls


def get_cache_names(cc_mirror):
    cache = os.path.expanduser('~/.cache/cdx_toolkit/')
    filename = re.sub(r'[^\w]', '_', cc_mirror.replace('https://', ''))
    return cache, filename


def check_collinfo_cache(cc_mirror):
    cache, filename = get_cache_names(cc_mirror)
    try:
        mtime = os.path.getmtime(cache + filename)
    except Exception as e:
        LOGGER.debug('unable to get collinfo cache mtime: '+repr(e))
        return
    if mtime > time.time() - 86400:
        try:
            LOGGER.debug('collinfo cache hit')
            with open(cache + filename) as fd:
                return json.load(fd)
        except Exception as e:
            LOGGER.debug('unable to read collinfo cache: '+repr(e))
    else:
        LOGGER.debug('collinfo cache too old')


def set_collinfo_cache(cc_mirror, collinfo):
    cache, filename = get_cache_names(cc_mirror)

    try:
        os.makedirs(cache, exist_ok=True)
        with open(cache + filename + '.new', 'w') as fd:
            fd.write(collinfo)
        os.rename(cache + filename + '.new', cache + filename)
        LOGGER.debug('collinfo cache written')
    except Exception as e:
        LOGGER.debug('problem writing collinfo cache: '+repr(e))


def get_cc_endpoints(cc_mirror):
    col = check_collinfo_cache(cc_mirror)
    if not col:
        url = cc_mirror.rstrip('/') + '/collinfo.json'
        r = myrequests_get(url)
        if r.status_code != 200:
            raise RuntimeError('error {} getting list of cc indices from {}'.format(r.status_code, collinfo))  # pragma: no cover
        set_collinfo_cache(cc_mirror, r.text)
        col = r.json()

    endpoints = [x['cdx-api'] for x in col]
    if len(endpoints) < 60:  # last seen to be 100
        raise ValueError('Surprisingly few endpoints for common crawl index')  # pragma: no cover
    LOGGER.info('Found %d endpoints in the Common Crawl index', len(endpoints))

    # endpoints arrive descending, make them ascending
    endpoints = sorted(endpoints)

    return endpoints


def apply_cc_defaults(params, crawl_present=False, now=None):
    # closest has needs
    #   if crawl, do nothing (expect the user to have picked the correct crawls)
    #     XXX ? check sort order, which happens later?
    #   if no from or to, set them -/+ 3 months from the closest timestamp
    # crawl? nothing
    # no crawl? 1 year if not specified

    if params.get('closest') is not None:
        closest_t = timestamp_to_time(params['closest'])
        three_months = 3 * 30 * 86400
        if params.get('from_ts') is None:
            params['from_ts'] = time_to_timestamp(closest_t - three_months)
            LOGGER.info('no from but closest, setting from=%s', params['from_ts'])
        if params.get('to') is None:
            params['to'] = time_to_timestamp(closest_t + three_months)
            LOGGER.info('no to but closest, setting to=%s', params['to'])
        # XXX set sort order to funky? which does not exist yet
    elif not crawl_present:
        # can't check params for 'crawl' because crawl is not ever set in params
        year = 365*86400
        if params.get('from_ts') is not None:
            if params.get('to') is None:
                #from_ts = pad_timestamp(params['from_ts'])
                #params['to'] = time_to_timestamp(timestamp_to_time(from_ts) + year)
                #LOGGER.info('no to, setting to=%s', params['to'])
                LOGGER.info('from but no to, not doing anything')
        elif params.get('to') is not None:
            if params.get('from_ts') is None:
                to = pad_timestamp_up(params['to'])
                params['from_ts'] = time_to_timestamp(timestamp_to_time(to) - year)
                LOGGER.info('to but no from_ts, setting from_ts=%s', params['from_ts'])
        else:
            if not now:
                # now is passed in by tests. if not set, use actual now.
                # XXX could be changed to mock
                now = time.time()
            params['from_ts'] = time_to_timestamp(now - year)
            LOGGER.info('no from or to, setting default 1 year ago from_ts=%s', params['from_ts'])
    else:
        # crawl -- assume the user picked the right things
        pass


def match_cc_crawls(crawls, raw_index_list):
    # match crawls requested on the command line to actual crawls
    # note that from/to are not considered here
    # crawls should be normalized so it's supposed to be a list of str
    if crawls is None:
        return raw_index_list
    if len(crawls) == 1 and crawls[0].isdigit():
        num = int(crawls[0])
        raw_index_list = raw_index_list[-num:]
    else:
        selected = set()
        used = set()
        for asked in crawls:
            for available in raw_index_list:
                if asked in available:
                    used.add(asked)
                    selected.add(available)
        if not used:
            raise ValueError('No matches for crawls '+','.join(crawls))
        missed = set(crawls).difference(used)
        if missed:
            LOGGER.warning('No matches for these crawl args: '+','.join(missed))
        raw_index_list = sorted(selected)
    LOGGER.info('matched crawls are: '+','.join(raw_index_list))
    return raw_index_list


def make_cc_maps(raw_index_list):
    # chainsaw all of the cc index names to a time, which we'll use as the end-time of its data

    endpoints = raw_index_list.copy()
    cc_times = []
    cc_map = {}
    for endpoint in endpoints:
        t = None
        m = re.search(r'CC-MAIN-(\d\d\d\d-\d\d)-', endpoint)
        if m:
            t = cc_index_to_time(m.group(1))
        m = re.search(r'CC-MAIN-(\d\d\d\d-\d\d\d\d)-', endpoint)
        if m:
            t = cc_index_to_time_special(m.group(1))
        m = re.search(r'CC-MAIN-(\d\d\d\d)-i', endpoint)
        if m:
            t = cc_index_to_time_special(m.group(1))
        if t is None:
            LOGGER.error('unable to parse date out of %s', endpoint)
            continue
        cc_times.append(t)
        cc_map[t] = endpoint
    return cc_map, sorted(cc_times)


def check_cc_from_to(params):
    # given caller's time specification, select from and to times; enforce limit on combinations
    # closest: both from and to must be present
    # otherwise: expect from to exist (due to the cc default 1 year)
    if 'closest' in params:
        if 'from_ts' not in params or params['from_ts'] is None:
            raise ValueError('Cannot happen')
        else:
            from_ts_t = timestamp_to_time(params['from_ts'])
        if 'to' not in params or params['to'] is None:
            raise ValueError('Cannot happen')
        else:
            to_t = timestamp_to_time(params['to'])
    else:
        if 'to' in params:
            to = pad_timestamp_up(params['to'])
            to_t = timestamp_to_time(to)
            if 'from_ts' not in params or params['from_ts'] is None:
                raise ValueError('Cannot happen')
            else:
                from_ts_t = timestamp_to_time(params['from_ts'])
        else:
            to_t = None
            if 'from_ts' not in params or params['from_ts'] is None:
                raise ValueError('Cannot happen')
            else:
                from_ts_t = timestamp_to_time(params['from_ts'])
    return from_ts_t, to_t


def bisect_cc(cc_map, cc_times, from_ts_t, to_t):
    # bisect to find the start and end of our cc indexes
    start = bisect.bisect_left(cc_times, from_ts_t) - 1
    start = max(0, start)
    if to_t is not None:
        end = bisect.bisect_right(cc_times, to_t) + 1
        end = min(end, len(cc_times))
    else:
        end = len(cc_times)
    return [cc_map[x] for x in cc_times[start:end]]


def filter_cc_endpoints(raw_index_list, cc_sort, params={}):
    crawl_present = False
    if 'crawl' in params:
        crawl_present = True
        crawls = params['crawl']
        del params['crawl']
        index_list = match_cc_crawls(crawls, raw_index_list)

    else:
        # date-based selection. if --crawl was specified, raw_index_list has already been narrowed
        # YYY this does not yet use collinfo.json from, to
        # YYY shouldn't this be skipped if crawl_present?
        cc_map, cc_times = make_cc_maps(raw_index_list)
        from_ts_t, to_t = check_cc_from_to(params)
        index_list = bisect_cc(cc_map, cc_times, from_ts_t, to_t)

        # write the fully-adjusted from and to into params XXX necessasry?
        # XXX wut? should we only do this when we've changed or added these ?!
        # to_t might have been padded. does from_ts ever get padded?
        params['from_ts'] = time_to_timestamp(from_ts_t)
        if to_t is not None:
            params['to'] = time_to_timestamp(to_t)

    # adjust index_list order based on cc_sort order
    if 'closest' in params:
        # XXX funky ordering not implemented, inform the caller
        # cli already prints a warning for iter + closest, telling user to use get instead
        # no need to warn if it's a single crawl
        # this routine is called for both get and iter
        pass
    if cc_sort == 'ascending':
        pass  # already in ascending order
    elif cc_sort == 'mixed':
        index_list.reverse()
    else:
        raise ValueError('unknown cc_sort arg of '+cc_sort)

    if index_list:
        if crawl_present:
            LOGGER.info('using cc crawls '+','.join(index_list))
        else:
            LOGGER.info('using cc index range from %s to %s', index_list[0], index_list[-1])
    else:
        LOGGER.warning('empty cc index range found')

    return index_list
