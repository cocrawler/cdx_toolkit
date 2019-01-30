import pytest

import cdx_toolkit


def test_munge_filter():
    tests = (('foo', 'foo', 'foo'),
             ('!status:200', '!statuscode:200', '!status:200'),
             ('statuscode:200', 'statuscode:200', 'status:200'),
             ('url:foo', 'original:foo', 'url:foo'))

    for t, ia, cc in tests:
        assert cdx_toolkit.munge_filter([t], 'ia') == [ia]
        assert cdx_toolkit.munge_filter([t], 'cc') == [cc]

    with pytest.raises(ValueError):
        assert cdx_toolkit.munge_filter(['!=status:200'], 'ia')


def test_munge_fields():
    tests = ((('statuscode', 'original'),
              ([200, 'http://example.com/'],),
              [{'status': 200, 'url': 'http://example.com/'}]),)

    for fields, lines, ret in tests:
        assert cdx_toolkit.munge_fields(fields, lines) == ret
