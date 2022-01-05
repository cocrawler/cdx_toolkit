import requests
import logging
import time
from urllib.parse import urlparse

from . import __version__

LOGGER = logging.getLogger(__name__)


previously_seen_hostnames = {
    'commoncrawl.s3.amazonaws.com',
    'web.archive.org',
    'web.archive.org',
}


def dns_fatal(url):
    '''We have a dns error, should we fail immediately or not?'''
    hostname = urlparse(url).hostname
    if hostname not in previously_seen_hostnames:
        return True


def myrequests_get(url, params=None, headers=None, cdx=False, allow404=False):
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
            if resp.status_code in {503, 502, 504, 500}:  # pragma: no cover
                # 503=slow down, 50[24] are temporary outages, 500=Amazon S3 generic error
                retries += 1
                if retries > 5:
                    LOGGER.warning('retrying after 1s for %d', resp.status_code)
                else:
                    LOGGER.info('retrying after 1s for %d', resp.status_code)
                time.sleep(1)
                continue
            if resp.status_code in {400, 404}:  # pragma: no cover
                LOGGER.info('response body is %s', resp.text)
                raise RuntimeError('invalid url of some sort, status={} {}'.format(resp.status_code, url))
            resp.raise_for_status()
            retry = False
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.Timeout) as e:
            connect_errors += 1
            string = '{} failures for url {} {!r}: {}'.format(connect_errors, url, params, str(e))

            if 'Name or service not known' in string:
                if dns_fatal(url):
                    raise ValueError('invalid hostname in url '+url) from None

            if connect_errors > 100:
                LOGGER.error(string)
                raise ValueError(string)
            if connect_errors > 10:
                LOGGER.warning(string)
            LOGGER.info('retrying after 1s for '+str(e))
            time.sleep(1)
        except requests.exceptions.RequestException as e:  # pragma: no cover
            LOGGER.warning('something unexpected happened, giving up after %s', str(e))
            raise

    hostname = urlparse(url).hostname
    if hostname not in previously_seen_hostnames:
        previously_seen_hostnames.add(hostname)

    return resp
