import requests
import logging
import os
import time

from . import __version__

LOGGER = logging.getLogger(__name__)


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
    connect_errors = 0
    while retry:
        try:
            LOGGER.debug('getting %s %r', url, params)
            resp = requests.get(url, params=params, headers=headers,
                                timeout=(30., 30.), allow_redirects=False)
            if cdx and resp.status_code in (400, 404):
                # 400: ia html error page -- probably page= is too big -- not an error
                # 404: pywb {'error': 'No Captures found for: www.pbxxxxxxm.com/*'} -- not an error
                LOGGER.debug('giving up with status %d, no captures found', resp.status_code)
                retry = False
                break
            if allow404 and resp.status_code == 404:
                retry = False
                break
            if resp.status_code in (503, 502, 504, 500):  # pragma: no cover
                # 503=slow down, 50[24] are temporary outages, 500=Amazon S3 generic error
                LOGGER.info('retrying after 1s for %d', resp.status_code)
                time.sleep(1)
                continue
            if resp.status_code in (400, 404):  # pragma: no cover
                LOGGER.info('funky response %d for url %s %r', resp.status_code, url, params)
                LOGGER.info('response body is %s', resp.text)
                raise RuntimeError('invalid url of some sort: '+url)
            resp.raise_for_status()
            retry = False
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.Timeout) as e:
            connect_errors += 1
            if connect_errors > 10:
                if os.getenv('CDX_TOOLKIT_TEST_REQUESTS'):
                    # used in tests/test.sh
                    print('DYING IN MYREQUEST_GET')
                    exit(0)
                else:  # pragma: no cover
                    LOGGER.error('Final failure for url %s %r', url, params)
                    raise
            LOGGER.warning('retrying after 1s for '+str(e))
            time.sleep(1)
        except requests.exceptions.RequestException as e:  # pragma: no cover
            LOGGER.warning('something unexpected happened, giving up after %s', str(e))
            raise
    return resp
