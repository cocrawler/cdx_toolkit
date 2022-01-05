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


def test_args():
    with pytest.raises(ValueError):
        cdx = cdx_toolkit.CDXFetcher(wb='foo', warc_url_prefix='foo')
    with pytest.raises(ValueError):
        cdx = cdx_toolkit.CDXFetcher(source='asdf')
    with pytest.raises(ValueError):
        cdx = cdx_toolkit.CDXFetcher(source='cc', wb='foo')
