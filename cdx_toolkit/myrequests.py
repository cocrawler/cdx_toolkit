import requests
import logging
import time
from urllib.parse import urlparse

from . import __version__

LOGGER = logging.getLogger(__name__)

previously_seen_hostnames = {
    'commoncrawl.s3.amazonaws.com',
    'data.commoncrawl.org',
    'web.archive.org',
}


def dns_fatal(hostname):
    '''We have a dns error, should we fail immediately or not?'''
    if hostname not in previously_seen_hostnames:
        return True


retry_info = {
    'default': {
        'next_fetch': 0,
        'minimum_interval': 3.0,
    },
    'index.commoncrawl.org': {
        'next_fetch': 0,
        'minimum_interval': 1.0,
    },
    'data.commoncrawl.org': {
        'next_fetch': 0,
        'minimum_interval': 0.55,
    },
    'web.archive.org': {
        'next_fetch': 0,
        'minimum_interval': 6.0,
    },
}


def get_retries(hostname):
    if hostname not in retry_info:
        retry_info[hostname] = retry_info['default'].copy()
        LOGGER.debug('initializing retry info for new host '+hostname)
    entry = retry_info[hostname]
    if not entry['next_fetch']:
        entry['next_fetch'] = time.time()
    return entry['next_fetch'], entry['minimum_interval']


def update_next_fetch(hostname, next_fetch):
    retry_info[hostname]['next_fetch'] = next_fetch


def myrequests_get(
    url, 
    params=None, 
    headers=None, 
    cdx=False, 
    allow404=False, 
    raise_error_after_n_errors: int = 1, 
    raise_warning_after_n_errors: int = 10,
    ):
    t = time.time()

    hostname = urlparse(url).hostname
    next_fetch, minimum_interval = get_retries(hostname)

    if t < next_fetch:
        dt = next_fetch - t
        if dt > 3.1:
            LOGGER.debug('sleeping for {:.3f}s before next fetch'.format(dt))
        time.sleep(dt)
    # next_fetch is also updated at the bottom
    update_next_fetch(hostname, next_fetch + minimum_interval)

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
        headers['User-Agent'] = 'pypi_cdx_toolkit/'+__version__

    retry = True
    retry_sec = 2 * minimum_interval
    retry_max_sec = 60
    retries = 0
    connect_errors = 0
    while retry:
        try:
            LOGGER.debug('getting %s %r', url, params)
            resp = requests.get(url, params=params, headers=headers,
                                timeout=(30., 30.), allow_redirects=False)
            if cdx and resp.status_code in {400, 404}:
                # 400: ia html error page -- probably page= is too big -- not an error
                # 404: pywb {'error': 'No Captures found for: www.pbxxxxxxm.com/*'} -- not an error
                LOGGER.debug('giving up with status %d, no captures found', resp.status_code)
                retry = False
                break
            if allow404 and resp.status_code == 404:
                retry = False
                break
            if resp.status_code in {429, 500, 502, 503, 504, 509}:  # pragma: no cover
                # 503=slow down, 50[24] are temporary outages, 500=Amazon S3 generic error
                # CC takes a 503 from storage and then emits a 500 with error text in resp.text
                # I have never seen IA or CC send 429 or 509, but just in case...
                # 429 is also a slow down, IA started sending them mid-2023
                retries += 1
                level = 30 if retries > 5 else 20  # 30=warning 20=info
                LOGGER.log(level, 'retrying after %.2fs for %d', retry_sec, resp.status_code)
                if resp.text:
                    LOGGER.log(level, 'response body is %s', resp.text)
                time.sleep(retry_sec)
                retry_sec = min(retry_sec*2, retry_max_sec)
                continue
            if resp.status_code in {400, 404}:  # pragma: no cover
                if resp.text:
                    LOGGER.info('response body is %s', resp.text)
                raise RuntimeError('invalid url of some sort, status={} {}'.format(resp.status_code, url))
            resp.raise_for_status()
            retry = False
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.Timeout) as e:
            connect_errors += 1
            string = '{} failures for url {} {!r}: {}'.format(connect_errors, url, params, str(e))

            # Check for DNS errors with different operating systems
            if (('Name or service not known' in string)  # linux
                or ('nodename nor servname provided, or not known' in string)  # macos
                or ('getaddrinfo failed' in string)):  # windows
                if dns_fatal(url):
                    raise ValueError('invalid hostname in url '+url) from None

            if connect_errors > raise_error_after_n_errors:
                LOGGER.error(string)
                raise ValueError(string)
            if connect_errors > raise_warning_after_n_errors:
                LOGGER.warning(string)
            LOGGER.info('retrying after {:.2f}s for '.format(retry_max_sec)+str(e))
            time.sleep(retry_max_sec)  # notice the extra-long sleep
            retry_sec = min(retry_sec*2, retry_max_sec)
        except requests.exceptions.RequestException as e:  # pragma: no cover
            LOGGER.warning('something unexpected happened, giving up after %s', str(e))
            raise

    hostname = urlparse(url).hostname
    if hostname not in previously_seen_hostnames:
        previously_seen_hostnames.add(hostname)

    # in case we had a lot of retries, etc
    update_next_fetch(hostname, time.time() + minimum_interval)

    return resp
