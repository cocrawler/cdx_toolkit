import requests
import logging
import os
import time

from . import __version__

LOGGER = logging.getLogger(__name__)


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
        headers['User-Agent'] = 'pypi_cdx_toolkit/'+__version__

    retry = True
    connect_errors = 0
    while retry:
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=(30., 30.))
            if resp.status_code == 400 and 'page' not in params:
                raise RuntimeError('invalid url of some sort: '+url)  # pragma: no cover
            if resp.status_code in (400, 404):
                LOGGER.info('giving up with status %d', resp.status_code)
                # 400: html error page -- probably page= is too big
                # 404: {'error': 'No Captures found for: www.pbxxxxxxm.com/*'} -- not an error
                retry = False
                break
            if resp.status_code in (503, 502, 504, 500):  # pragma: no cover
                # 503=slow down, 50[24] are temporary outages, 500=Amazon S3 generic error
                LOGGER.info('retrying after 1s for %d', resp.status_code)
                time.sleep(1)
                continue
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
                    print('Final failure for url='+url)
                    raise
            LOGGER.warning('retrying after 1s for '+str(e))
            time.sleep(1)
        except requests.exceptions.RequestException as e:  # pragma: no cover
            LOGGER.warning('something unexpected happened, giving up after %s', str(e))
            raise
    return resp
