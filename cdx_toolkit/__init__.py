import logging
import re
import time
import datetime
import bisect
import json
import gzip
#import hashlib
from urllib.parse import quote
from pkg_resources import get_distribution, DistributionNotFound

__version__ = 'installed-from-git'

from .myrequests import myrequests_get
from .timestamp import time_to_timestamp, timestamp_to_time, pad_timestamp_up
from .compat import munge_fields, munge_filter
from .commoncrawl import get_cc_endpoints, apply_cc_defaults, fetch_warc_content

LOGGER = logging.getLogger(__name__)

try:
    # this works for the pip-installed package
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    pass


lines_per_page = 3000  # no way to get this from the API without fetching a page


def showNumPages(r):
    j = r.json()
    if isinstance(j, dict):  # pywb always returns json
        pages = int(j.get('blocks', 0))
    elif isinstance(j, int):  # ia always returns text, parsed as a json int
        pages = j
    else:
        raise ValueError('surprised by showNumPages value of '+str(j))
    return pages


def pages_to_samples(pages):
    # adjust pages for the partial page at the start and end
    if pages > 1:
        pages = pages - 1.0
    elif pages >= 1:
        pages = pages - 0.5
    pages *= lines_per_page
    return int(pages)


def cdx_to_json(resp):
    if resp.status_code == 404:
        # this is an empty result for pywb iff {"error": "No Captures found for: ..."}
        if resp.text.startswith('{'):
            j = json.loads(resp.text)
            if 'error' in j:
                return []
        raise ValueError('404 seen for API call, did you configure the endpoint correctly?')

    text = resp.text

    if text.startswith('{'):  # pywb output='json' is jsonl
        lines = resp.text.splitlines()
        ret = []
        for l in lines:
            ret.append(json.loads(l))
        return ret

    # ia output='json' is a json list of lists
    if not text.startswith('['):
        raise ValueError('cannot decode response, first bytes are '+repr(text[:50]))  # pragma: no cover
    if text.startswith('[]'):
        return []

    try:
        lines = json.loads(text)
        fields = lines.pop(0)  # first line is the list of field names
    except (json.decoder.JSONDecodeError, KeyError, IndexError):  # pragma: no cover
        raise ValueError('cannot decode response, first bytes are '+repr(text[:50]))

    return munge_fields(fields, lines)


def fetch_wb_content(capture, modifier='id_', prefix='https://web.archive.org/web'):
    if 'url' not in capture or 'timestamp' not in capture:
        raise ValueError('capture must contain an url and timestamp')

    fetch_url = capture['url']
    timestamp = capture['timestamp']

    url = '{}/{}{}/{}'.format(prefix, timestamp, modifier, quote(fetch_url))

    resp = myrequests_get(url)
    # This is bytes, but IA did set the Content-Type: for us... resp.text
    # requests will apply content-type with errors='replace' before calling
    # chardet, which is both kinda wrong and very slow.
    content_bytes = resp.content
    return content_bytes


class CDXFetcherIter:
    def __init__(self, cdxfetcher, params={}, index_list=None):
        self.cdxfetcher = cdxfetcher
        self.params = params
        if 'page' in params:
            raise ValueError('must not set page= in a CDX iterator')
        self.endpoint = 0
        self.page = -1
        self.params['page'] = self.page
        self.cdx_objs = []
        self.index_list = index_list

        self.get_more()

    def get_more(self):
        while True:
            self.page += 1
            status, objs = self.cdxfetcher.get_for_iter(self.endpoint, self.page,
                                                        params=self.params, index_list=self.index_list)
            if status == 'last endpoint':
                LOGGER.info('get_more: I have reached the end')
                return  # caller will raise StopIteration
            if status == 'last page':
                LOGGER.info('get_more: moving to next endpoint')
                self.endpoint += 1
                self.page = -1
                continue
            LOGGER.info('get_more, got %d more objs', len(objs))
            self.cdx_objs.extend(objs)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                return self.cdx_objs.pop(0)
            except IndexError:
                LOGGER.info('getting more in __next__')
                self.get_more()
                if len(self.cdx_objs) <= 0:
                    raise StopIteration


class CDXFetcher:
    def __init__(self, source='cc', cc_sort='mixed', loglevel=None):
        self.source = source
        self.cc_sort = cc_sort
        self.source = source

        if source == 'cc':
            self.raw_index_list = get_cc_endpoints()
        elif source == 'ia':
            self.index_list = ('https://web.archive.org/cdx/search/cdx',)
        elif source.startswith('https://') or source.startswith('http://'):
            self.index_list = (source,)
        else:
            raise ValueError('could not understand source')

        if loglevel:
            LOGGER.setLevel(level=loglevel)

    def customize_index_list(self, params):
        if self.source == 'cc' and ('from' in params or 'from_ts' in params or 'to' in params):
            LOGGER.info('making a custom cc index list')
            return self.filter_cc_endpoints(params=params)
        else:
            return self.index_list

    def filter_cc_endpoints(self, params={}):
        endpoints = self.raw_index_list.copy()

        # chainsaw all of the cc index names to a time, which we'll use as the end-time of its data
        cc_times = []
        cc_map = {}
        timestamps = re.findall(r'CC-MAIN-(\d\d\d\d-\d\d)', ''.join(endpoints))
        CC_TIMESTAMP = '%Y-%W-%w'  # I think these are ISO weeks
        for timestamp in timestamps:
            utc = datetime.timezone.utc
            t = datetime.datetime.strptime(timestamp+'-0', CC_TIMESTAMP).replace(tzinfo=utc).timestamp()
            cc_times.append(t)
            cc_map[t] = endpoints.pop(0)
        # now I'm set up to bisect in cc_times and then index into cc_map to find the actual endpoint

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
            end = min(end, len(self.raw_index_list))
        else:
            end = len(self.raw_index_list)

        index_list = self.raw_index_list[start:end]
        params['from_ts'] = time_to_timestamp(from_ts_t)
        if to_t is not None:
            params['to'] = time_to_timestamp(to_t)

        if 'closest' in params:
            pass
            # XXX funky ordering

        if self.cc_sort == 'ascending':
            pass  # already in ascending order
        elif self.cc_sort == 'mixed':
            index_list.reverse()
        else:
            raise ValueError('unknown cc_sort arg of '+self.cc_sort)

        if index_list:
            LOGGER.info('using cc index range from %s to %s', index_list[0], index_list[-1])
        else:
            LOGGER.warning('empty cc index range found')

        return index_list

    def get(self, url, **kwargs):
        # from_ts=None, to=None, matchType=None, limit=None, sort=None, closest=None,
        # filter=None, fl=None, page=None, pageSize=None, showNumPages=None):
        params = kwargs
        params['url'] = url
        params['output'] = 'json'
        if 'filter' in params:
            params['filter'] = munge_filter(params['filter'], self.source)

        if 'limit' not in params:
            params['limit'] = 1000
        if self.source == 'cc':
            apply_cc_defaults(params)

        index_list = self.customize_index_list(params)

        ret = []
        for endpoint in index_list:
            resp = myrequests_get(endpoint, params=params)
            objs = cdx_to_json(resp)  # turns 400 and 404 into []
            ret.extend(objs)
            if 'limit' in params:
                params['limit'] -= len(objs)
                if params['limit'] <= 0:
                    break
        return ret

    def items(self, url, **kwargs):
        params = kwargs
        params['url'] = url
        params['output'] = 'json'
        if 'filter' in params:
            params['filter'] = munge_filter(params['filter'], self.source)

        if 'limit' not in params:
            params['limit'] = 1000
        if self.source == 'cc':
            apply_cc_defaults(params)

        index_list = self.customize_index_list(params)
        return CDXFetcherIter(self, params=params, index_list=index_list)

    def get_for_iter(self, endpoint, page, params={}, index_list=None):
        '''
        Specalized get for the iterator
        '''
        if endpoint >= len(index_list):
            return 'last endpoint', []
        if params.get('limit', -1) == 0:
            return 'last endpoint', []  # a little white lie

        endpoint = index_list[endpoint]
        params['page'] = page
        resp = myrequests_get(endpoint, params=params)
        if resp.status_code == 400:  # pywb
            return 'last page', []
        if resp.text == '':  # ia
            return 'last page', []

        ret = cdx_to_json(resp)  # turns 404 into []
        if 'limit' in params:
            params['limit'] -= len(ret)
        return 'ok', ret

    def get_size_estimate(self, url, as_pages=False, **kwargs):
        '''
        Get the number of pages that match url

        useful additional args: matchType='host' pageSize=1
        or, url can end with * or start with *. to set the matchType
        '''

        params = {'url': url, 'showNumPages': 'true'}
        params.update(**kwargs)
        if self.source == 'cc':
            apply_cc_defaults(params)

        index_list = self.customize_index_list(params)

        pages = 0
        for endpoint in index_list:
            resp = myrequests_get(endpoint, params=params)
            if resp.status_code == 200:
                pages += showNumPages(resp)
            else:
                pass  # silently ignore empty answers  # pragma: no cover

        if not as_pages:
            pages = pages_to_samples(pages)
        return pages
