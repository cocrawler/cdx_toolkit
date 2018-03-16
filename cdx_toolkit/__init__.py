import requests
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
import os

__version__ = 'installed-from-git'

LOGGER = logging.getLogger(__name__)

try:
    # this works for the pip-installed package
    version = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    pass


def myrequests_get(url, params=None, headers=None):
    if params:
        if 'from_ts' in params:
            params['from'] = params['from_ts']
            del params['from_ts']
        if 'limit' in params:
            if not isinstance(params['limit'], int):
                # this needs to be an int because we subtract from it elsewhere
                params['limit'] = int(params['limit'])

    if headers is None:
        headers = {}
    if 'user-agent' not in headers:
        headers['user-agent'] = 'pypi_cdx_toolkit/'+__version__

    retry = True
    connect_errors = 0
    while retry:
        try:
            resp = requests.get(url, params=params, headers=headers)
            if resp.status_code == 400 and 'page' not in params:
                raise RuntimeError('invalid url of some sort: '+url)  # pragma: no cover
            if resp.status_code in (400, 404):
                LOGGER.debug('giving up with status %d', resp.status_code)
                # 400: html error page -- probably page= is too big
                # 404: {'error': 'No Captures found for: www.pbxxxxxxm.com/*'} -- not an error
                retry = False
                break
            if resp.status_code in (503, 502, 504):  # 503=slow down, 50[24] are temporary outages  # pragma: no cover
                LOGGER.debug('retrying after 1s for %d', resp.status_code)
                time.sleep(1)
                continue
            resp.raise_for_status()
            retry = False
        except requests.exceptions.ConnectionError:
            connect_errors += 1
            if connect_errors > 10:
                if os.getenv('CDX_TOOLKIT_TEST_REQUESTS'):
                    print('DYING IN MYREQUEST_GET')
                    exit(0)
                else:
                    raise
            LOGGER.warning('retrying after 1s for ConnectionError')
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            LOGGER.warning('something unexpected happened, giving up after %s', str(e))
            raise
    return resp


fields_to_cc = {'statuscode': 'status', 'original': 'url', 'mimetype': 'mime'}
fields_to_ia = dict([(v, k) for k, v in fields_to_cc.items()])


def munge_filter(filter, source):
    if source == 'ia':
        for bad in ('=', '!=',  '~',  '!~'):
            if filter.startswith(bad):
                raise ValueError('ia does not support the filter '+bad)
        for k, v in fields_to_ia.items():
            filter = re.sub(r'\b'+k+':', v+':', filter, 1)
    if source == 'cc':
        for k, v in fields_to_cc.items():
            filter = re.sub(r'\b'+k+':', v+':', filter, 1)
    return filter


def get_cc_endpoints():
    # TODO: cache me
    r = myrequests_get('http://index.commoncrawl.org/collinfo.json')
    if r.status_code != 200:
        raise RuntimeError('error getting list of common crawl indices: '+str(r.status_code))  # pragma: no cover

    j = r.json()
    endpoints = [x['cdx-api'] for x in j]
    if len(endpoints) < 30:  # last seen to be 39
        raise ValueError('Surprisingly few endoints for common crawl index')  # pragma: no cover

    # endpoints arrive sorted oldest to newest, but let's force that anyawy
    endpoints = sorted(endpoints)

    return endpoints


lines_per_page = 3000  # no way to get this from the API without fetching a page


def showNumPages(r):
    j = r.json()
    if isinstance(j, dict):  # pywb always returns json
        pages = int(j.get('blocks', 0))
    elif isinstance(j, int):  # ia always returns text, parsed as a json int
        pages = j
    else:
        raise ValueError('surprised by showNumPages value of '+str(j))  # pragma: no cover
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
        return []

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

    ret = []
    for l in lines:
        obj = {}
        for f in fields:
            value = l.pop(0)
            if f in fields_to_cc:
                obj[fields_to_cc[f]] = value
            else:
                obj[f] = value
        ret.append(obj)
    return ret


# confusingly, python's documentation refers to their float version
# of the unix time as a 'timestamp'. This code uses 'timestamp' to
# mean the CDX concept of timestamp.

TIMESTAMP = '%Y%m%d%H%M%S'
TIMESTAMP_LOW = '19780101000000'
TIMESTAMP_HIGH = '29991231235959'


def pad_timestamp(timestamp):
    return timestamp + TIMESTAMP_LOW[len(timestamp):]


days_in_month = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def pad_timestamp_up(timestamp):
    timestamp = timestamp + TIMESTAMP_HIGH[len(timestamp):]
    month = timestamp[4:6]
    timestamp = timestamp[:6] + str(days_in_month[int(month)]) + timestamp[8:]
    return timestamp


def timestamp_to_time(timestamp):
    utc = datetime.timezone.utc
    timestamp = pad_timestamp(timestamp)
    try:
        return datetime.datetime.strptime(timestamp, TIMESTAMP).replace(tzinfo=utc).timestamp()
    except ValueError:
        LOGGER.error('cannot parse timestamp, is it a legal date?: '+timestamp)
        raise


def time_to_timestamp(t):
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc).strftime(TIMESTAMP)


def apply_cc_defaults(params):
    if 'from_ts' not in params or params['from_ts'] is None:
        year = 365*86400
        if 'to' in params and params['to'] is not None:
            to = pad_timestamp_up(params['to'])
            params['from_ts'] = time_to_timestamp(timestamp_to_time(to) - year)
            LOGGER.debug('no from but to, setting from=%s', params['from_ts'])
        else:
            params['from_ts'] = time_to_timestamp(time.time() - year)
            LOGGER.debug('no from, setting from=%s', params['from_ts'])


def fetch_warc_content(capture):
    filename = capture['filename']
    offset = int(capture['offset'])
    length = int(capture['length'])

    cc_external_prefix = 'https://commoncrawl.s3.amazonaws.com'
    url = cc_external_prefix + '/' + filename
    headers = {'Range': 'bytes={}-{}'.format(offset, offset+length-1)}

    resp = myrequests_get(url, headers=headers)
    content_bytes = resp.content

    # WARC digests can be represented in multiple ways (rfc 3548)
    # I have code in a pullreq for warcio that does this comparison
    #if 'digest' in capture and capture['digest'] != hashlib.sha1(content_bytes).hexdigest():
    #    LOGGER.error('downloaded content failed digest check')

    if content_bytes[:2] == b'\x1f\x8b':
        content_bytes = gzip.decompress(content_bytes)

    # hack the WARC response down to just the content_bytes
    try:
        warcheader, httpheader, content_bytes = content_bytes.strip().split(b'\r\n\r\n', 2)
    except ValueError:  # not enough values to unpack
        return b''

    # XXX help out with the page encoding? complicated issue.
    return content_bytes


def fetch_wb_content(capture):
    if 'url' not in capture or 'timestamp' not in capture:
        raise ValueError('capture must contain an url and timestamp')

    fetch_url = capture['url']
    timestamp = capture['timestamp']

    prefix = 'https://web.archive.org/web'
    url = '{}/{}{}/{}'.format(prefix, timestamp, 'js_', quote(fetch_url))

    resp = myrequests_get(url)
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
                LOGGER.debug('get_more: I have reached the end')
                return  # caller will raise StopIteration
            if status == 'last page':
                LOGGER.debug('get_more: moving to next endpoint')
                self.endpoint += 1
                self.page = -1
                continue
            LOGGER.debug('get_more, got %d more objs', len(objs))
            self.cdx_objs.extend(objs)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                return self.cdx_objs.pop(0)
            except IndexError:
                LOGGER.debug('getting more in __next__')
                self.get_more()
                if len(self.cdx_objs) <= 0:
                    raise StopIteration


class CDXFetcher:
    def __init__(self, source='cc', cc_sort='mixed'):
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

    def customize_index_list(self, params):
        if self.source == 'cc' and ('from' in params or 'from_ts' in params or 'to' in params):
            LOGGER.debug('making a custom cc index list')
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
            closest_t = timestamp_to_time(params['closest'])
            if 'from_ts' not in params or params['from_ts'] is None:
                # not provided, make 3 months earlier
                from_ts_t = closest_t - 3 * 30 * 86400
            else:
                from_ts_t = timestamp_to_time(params['from_ts'])
            if 'to' not in params or params['to'] is None:
                # not provided, make 3 months later
                to_t = closest_t + 3 * 30 * 86400
            else:
                to_t = timestamp_to_time(params['to'])
        else:
            if 'to' in params:
                to = pad_timestamp_up(params['to'])
                to_t = timestamp_to_time(to)
                if 'from_ts' not in params or params['from_ts'] is None:
                    from_ts_t = to_t - 365 * 86400
                else:
                    from_ts_t = timestamp_to_time(params['from_ts'])
            else:
                to_t = None
                if 'from_ts' not in params or params['from_ts'] is None:
                    from_ts_t = time.time() - 365 * 86400
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
