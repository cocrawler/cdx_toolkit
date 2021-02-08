'''
Code specific to accessing the Common Crawl index
'''
import time
import re
import bisect

import logging

from .myrequests import myrequests_get
from .timeutils import time_to_timestamp, timestamp_to_time, pad_timestamp_up, cc_index_to_time, cc_index_to_time_special

LOGGER = logging.getLogger(__name__)


def get_cc_endpoints(cc_mirror):
    collinfo = cc_mirror.rstrip('/') + '/collinfo.json'
    r = myrequests_get(collinfo)
    if r.status_code != 200:
        raise RuntimeError('error {} getting list of cc indices from {}'.format(r.status_code, collinfo))  # pragma: no cover

    j = r.json()
    endpoints = [x['cdx-api'] for x in j]
    if len(endpoints) < 30:  # last seen to be 39
        raise ValueError('Surprisingly few endpoints for common crawl index')  # pragma: no cover
    LOGGER.info('Found %d endpoints in the Common Crawl index', len(endpoints))

    # endpoints arrive sorted oldest to newest, but let's force that anyawy
    endpoints = sorted(endpoints)

    return endpoints


def apply_cc_defaults(params, now=None):
    three_months = 3 * 30 * 86400
    year = 365*86400
    if params.get('from_ts') is None:
        if params.get('closest') is not None:
            closest_t = timestamp_to_time(params['closest'])
            params['from_ts'] = time_to_timestamp(closest_t - three_months)
            LOGGER.info('no from but closest, setting from=%s', params['from_ts'])
            if params.get('to') is None:
                params['to'] = time_to_timestamp(closest_t + three_months)
                LOGGER.info('no to but closest, setting to=%s', params['to'])
        elif params.get('to') is not None:
            to = pad_timestamp_up(params['to'])
            params['from_ts'] = time_to_timestamp(timestamp_to_time(to) - year)
            LOGGER.info('no from but to, setting from=%s', params['from_ts'])
        else:
            if not now:
                now = time.time()
            params['from_ts'] = time_to_timestamp(now - year)
            LOGGER.info('no from, setting from=%s', params['from_ts'])
    if params.get('to') is None:
        if params.get('closest') is not None:
            closest_t = timestamp_to_time(params['closest'])
            # 3 months later
            params['to'] = time_to_timestamp(closest_t + three_months)
            LOGGER.info('no to but closest, setting from=%s', params['to'])
        else:
            # no to or closest; from was set above, we will not set to
            pass


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
    cc_map, cc_times = make_cc_maps(raw_index_list)

    from_ts_t, to_t = check_cc_from_to(params)

    index_list = bisect_cc(cc_map, cc_times, from_ts_t, to_t)

    # write the fully-adjusted from and to into params XXX necessasry?
    # XXX wut? should we only do this when we've changed or added these ?!
    params['from_ts'] = time_to_timestamp(from_ts_t)
    if to_t is not None:
        params['to'] = time_to_timestamp(to_t)

    # adjust index_list order based on cc_sort order
    if 'closest' in params:
        # XXX funky ordering not implemented, inform the caller
        # cli already prints a warning for iter + closer, telling user to use get instead
        # this routine is called for both get and iter
        pass
    if cc_sort == 'ascending':
        pass  # already in ascending order
    elif cc_sort == 'mixed':
        index_list.reverse()
    else:
        raise ValueError('unknown cc_sort arg of '+cc_sort)

    if index_list:
        LOGGER.info('using cc index range from %s to %s', index_list[0], index_list[-1])
    else:
        LOGGER.warning('empty cc index range found')

    return index_list
