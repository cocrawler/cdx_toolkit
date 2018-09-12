'''
Code specific to accessing the Common Crawl index
'''
import time
import re
import bisect

import logging

from .myrequests import myrequests_get
from .timeutils import time_to_timestamp, timestamp_to_time, pad_timestamp_up, cc_index_to_time

LOGGER = logging.getLogger(__name__)


def get_cc_endpoints():
    r = myrequests_get('https://index.commoncrawl.org/collinfo.json')
    if r.status_code != 200:
        raise RuntimeError('error getting list of common crawl indices: '+str(r.status_code))  # pragma: no cover

    j = r.json()
    endpoints = [x['cdx-api'] for x in j]
    if len(endpoints) < 30:  # last seen to be 39
        raise ValueError('Surprisingly few endpoints for common crawl index')  # pragma: no cover

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


def filter_cc_endpoints(raw_index_list, cc_sort, params={}):
    endpoints = raw_index_list.copy()

    # chainsaw all of the cc index names to a time, which we'll use as the end-time of its data

    cc_times = []
    cc_map = {}
    timestamps = re.findall(r'CC-MAIN-(\d\d\d\d-\d\d)', ''.join(endpoints))
    for timestamp in timestamps:
        t = cc_index_to_time(timestamp)
        cc_times.append(t)
        cc_map[t] = endpoints.pop(0)

    # bisect in cc_times and then index into cc_map to find the actual endpoint

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

    # bisect to find the start and end of our cc indexes
    start = bisect.bisect_left(cc_times, from_ts_t) - 1
    start = max(0, start)
    if to_t is not None:
        end = bisect.bisect_right(cc_times, to_t) + 1
        end = min(end, len(raw_index_list))
    else:
        end = len(raw_index_list)

    index_list = raw_index_list[start:end]
    params['from_ts'] = time_to_timestamp(from_ts_t)
    if to_t is not None:
        params['to'] = time_to_timestamp(to_t)

    if 'closest' in params:
        pass
        # XXX funky ordering

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
