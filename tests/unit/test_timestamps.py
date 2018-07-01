import pytest

import cdx_toolkit.timestamp as timestamp


def test_padding():
    assert timestamp.pad_timestamp('1998') == '19980101000000'
    assert timestamp.pad_timestamp_up('199802') == '19980228235959'
    assert timestamp.pad_timestamp_up('199812') == '19981231235959'


def test_time_timestamp():
    tests = (
        ('1978', 252460800.0),  # tests our timezone
        ('1999', 915148800.0),
        ('19981231235959', 915148799.0),  # previous line minus 1s
    )
    for ts, t in tests:
        assert timestamp.timestamp_to_time(ts) == t
        assert timestamp.time_to_timestamp(t) == timestamp.pad_timestamp(ts)

    with pytest.raises(ValueError):
        timestamp.timestamp_to_time('19990231')  # invalid day of month
