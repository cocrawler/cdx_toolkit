import datetime
from email.utils import parsedate
import logging

LOGGER = logging.getLogger(__name__)

# confusingly, python's documentation refers to their float version
# of the unix time as a 'timestamp'. This code uses 'timestamp' to
# mean the CDX concept of timestamp.

TIMESTAMP = '%Y%m%d%H%M%S'
TIMESTAMP_LOW = '19780101000000'
TIMESTAMP_HIGH = '29991231235959'

# if you ask for Feb we'll pad it to the 28th even if it's a leap year
days_in_month = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def pad_timestamp(ts):
    '''
    >>> pad_timestamp('1998')
    '19980101000000'
    '''
    return ts + TIMESTAMP_LOW[len(ts):]


def pad_timestamp_up(ts):
    '''
    >>> pad_timestamp_up('199802')
    '19980228235959'
    '''
    ts = ts + TIMESTAMP_HIGH[len(ts):]
    month = ts[4:6]
    ts = ts[:6] + str(days_in_month[int(month)]) + ts[8:]
    return ts


def timestamp_to_time(ts):
    '''
    >>> timestamp_to_time('1999')
    915148800.0
    '''
    utc = datetime.timezone.utc
    ts = pad_timestamp(ts)
    try:
        return datetime.datetime.strptime(ts, TIMESTAMP).replace(tzinfo=utc).timestamp()
    except ValueError:
        LOGGER.error('cannot parse timestamp, is it a legal date?: '+ts)
        raise


def time_to_timestamp(t):
    '''
    >>> time_to_timestamp(915148800.0)
    '19990101000000'
    '''
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc).strftime(TIMESTAMP)


CC_TIMESTAMP = '%Y-%W-%w'


def cc_index_to_time(cc):
    '''
    Convert a Commoncrawl index name YYYY-isoweek to a unixtime

    >>> cc_index_to_time('2018-02')
    1515888000.0
    '''
    utc = datetime.timezone.utc
    return datetime.datetime.strptime(cc+'-0', CC_TIMESTAMP).replace(tzinfo=utc).timestamp()


'''
Code cribbed from warcio/timeutls.py
'''

ISO_DT = '%Y-%m-%dT%H:%M:%SZ'


def http_date_to_datetime(string):
    """
    >>> http_date_to_datetime('Thu, 26 Dec 2013 09:50:10 GMT')
    datetime.datetime(2013, 12, 26, 9, 50, 10)
    """
    return datetime.datetime(*parsedate(string)[:6])


def datetime_to_iso_date(the_datetime):
    """
    >>> datetime_to_iso_date(datetime.datetime(2013, 12, 26, 10, 11, 12))
    '2013-12-26T10:11:12Z'

    >>> datetime_to_iso_date( datetime.datetime(2013, 12, 26, 10, 11, 12))
    '2013-12-26T10:11:12Z'
    """

    return the_datetime.strftime(ISO_DT)
