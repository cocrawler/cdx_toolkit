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
