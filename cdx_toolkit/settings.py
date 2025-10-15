import os

MAX_ERRORS = int(os.environ.get('CDXT_MAX_ERRORS', 100))
WARNING_AFTER_N_ERRORS = int(os.environ.get('CDXT_WARNING_AFTER_N_ERRORS', 10))

DEFAULT_MIN_RETRY_INTERVAL = float(os.environ.get('CDXT_DEFAULT_MIN_RETRY_INTERVAL', 3.0))
CC_INDEX_MIN_RETRY_INTERVAL = float(os.environ.get('CDXT_CC_INDEX_MIN_RETRY_INTERVAL', 1.0))
CC_DATA_MIN_RETRY_INTERVAL = float(os.environ.get('CDXT_CC_DATA_MIN_RETRY_INTERVAL', 0.55))
IA_MIN_RETRY_INTERVAL = float(os.environ.get('CDXT_IA_MIN_RETRY_INTERVAL', 6.0))


def get_mock_time():
    """Get the mock time from environment variable, evaluated dynamically"""
    mock_time = os.environ.get('CDXT_MOCK_TIME')
    return float(mock_time) if mock_time else None
