import cdx_toolkit


def test_padding():
    assert cdx_toolkit.pad_timestamp('1998') == '19980101000000'
    assert cdx_toolkit.pad_timestamp_up('199802') == '19980228235959'
    assert cdx_toolkit.pad_timestamp_up('199812') == '19981231235959'


def test_time_timestamp():
    tests = (
        ('1978', 252460800.0),  # tests our timezone
        ('1999', 915148800.0),
        ('19981231235959', 915148799.0),  # previous line minus 1s
    )
    for ts, t in tests:
        assert cdx_toolkit.timestamp_to_time(ts) == t
        assert cdx_toolkit.time_to_timestamp(t) == cdx_toolkit.pad_timestamp(ts)
