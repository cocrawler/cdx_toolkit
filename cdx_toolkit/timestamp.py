import datetime
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
    return ts + TIMESTAMP_LOW[len(ts):]


def pad_timestamp_up(ts):
    ts = ts + TIMESTAMP_HIGH[len(ts):]
    month = ts[4:6]
    ts = ts[:6] + str(days_in_month[int(month)]) + ts[8:]
    return ts


def timestamp_to_time(ts):
    utc = datetime.timezone.utc
    ts = pad_timestamp(ts)
    try:
        return datetime.datetime.strptime(ts, TIMESTAMP).replace(tzinfo=utc).timestamp()
    except ValueError:
        LOGGER.error('cannot parse timestamp, is it a legal date?: '+ts)
        raise


def time_to_timestamp(t):
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc).strftime(TIMESTAMP)
