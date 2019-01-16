import pytest
import six

import cdx_toolkit


def test_capture_object():
    cdx_cc = cdx_toolkit.CDXFetcher(source='cc')
    cdx_ia = cdx_toolkit.CDXFetcher(source='ia')
    cdx_only = cdx_toolkit.CDXFetcher(source='https://web.archive.org/cdx/search/cdx', loglevel='DEBUG')

    url = 'example.com'
    kwargs = {'limit': 1}

    got_one = False
    for obj in cdx_only.iter(url, **kwargs):
        got_one = True
        with pytest.raises(ValueError):
            _ = obj.content
    assert got_one

    for cdx in (cdx_cc, cdx_ia):
        got_one = False
        for obj in cdx.iter(url, **kwargs):
            got_one = True
            content = obj.content
            assert len(content) > 100
            assert isinstance(content, six.binary_type)

            content2 = obj.content
            assert content == content2

            r = obj.fetch_warc_record()
            r2 = obj.fetch_warc_record()
            assert r == r2

            stream = obj.content_stream
            # we read the stream above, so it's at eof
            more_content = stream.read()
            assert len(more_content) == 0

            text = obj.text
            assert isinstance(text, six.string_types)
            text2 = obj.text
            assert text == text2

            # some duck-type dict texts on obj
            obj['foo'] = 'asdf'
            assert obj['foo'] == 'asdf'
            assert 'foo' in obj
            del obj['foo']

        assert got_one
