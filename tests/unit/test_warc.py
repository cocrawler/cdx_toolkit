import cdx_toolkit.warc


def test_wb_redir_to_original():
    location = 'https://web.archive.org/web/20110209062054id_/http://commoncrawl.org/'
    ret = 'http://commoncrawl.org/'
    assert cdx_toolkit.warc.wb_redir_to_original(location) == ret
