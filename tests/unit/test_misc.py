import pytest

import cdx_toolkit


class MockResp:
    def __init__(self, thing):
        self.thing = thing

    def json(self):
        return self.thing


def test_showNumPages():
    j_cc = MockResp({'blocks': 3})
    assert cdx_toolkit.showNumPages(j_cc) == 3

    j_ia = MockResp(3)
    assert cdx_toolkit.showNumPages(j_ia) == 3

    with pytest.raises(ValueError):
        j_bad = MockResp('3')
        assert cdx_toolkit.showNumPages(j_bad) == 3


def test_munge_filter():
    tests = (('foo', 'foo', 'foo'),
             ('!status:200', '!statuscode:200', '!status:200'),
             ('statuscode:200', 'statuscode:200', 'status:200'),
             ('url:foo', 'original:foo', 'url:foo'))

    for t, ia, cc in tests:
        assert cdx_toolkit.munge_filter(t, 'ia') == ia
        assert cdx_toolkit.munge_filter(t, 'cc') == cc

    with pytest.raises(ValueError):
        assert cdx_toolkit.munge_filter('!=status:200', 'ia')
