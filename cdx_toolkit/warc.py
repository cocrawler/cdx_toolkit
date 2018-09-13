from urllib.parse import quote
import gzip
from io import BytesIO
import os.path
import datetime

# XXX make this optional?
from warcio import WARCWriter

from .myrequests import myrequests_get
from .timeutils import http_date_to_datetime, datetime_to_iso_date


def fake_wb_warc(wb_url, resp, capture):
    '''
    Given a playback from a wayback, fake up a warc response record
    '''
    httpheaders = []
    httpdate = None
    for k, v in resp.headers.items():
        kl = k.lower()
        if kl.startswith('x-archive-orig-date'):
            httpdate = v

        if kl.startswith('x-archive-orig-'):
            k = k[len('x-archive-orig-'):]
            httpheaders.append((k, v))
        elif kl == 'content-type':
            httpheaders.append(('Content-Type', v))
        else:
            if not kl.startswith('x-archive-'):
                k = 'X-Archive-' + k
            httpheaders.append((k, v))

    httpheaders = '\r\n'.join([h+': '+v for h, v in httpheaders])
    httpheaders = 'HTTP/1.1 {} OK\r\n'.format(capture['status']) + httpheaders + '\r\n'
    httpheaders = httpheaders.encode()

    warcheaders = b''
    warcheaders = [
        b'WARC/1.0',
        b'WARC-Source-URI: ' + wb_url.encode(),
        b'WARC-Creation-Date: ' + datetime_to_iso_date(datetime.datetime.now()).encode()
    ]
    if httpdate:
        warcheaders.append(b'WARC-Date: ' + datetime_to_iso_date(http_date_to_datetime(httpdate)).encode())
    warcheaders = b'\r\n'.join(warcheaders)

    content_bytes = resp.content

    return warcheaders, httpheaders, content_bytes


def fetch_wb_warc(capture, wb, modifier='id_'):
    for field in ('url', 'timestamp', 'status'):
        if field not in capture:
            raise ValueError('capture must contain '+field)

    if wb is None:
        raise ValueError('No wayback configured')

    url = capture['url']
    timestamp = capture['timestamp']

    wb_url = '{}/{}{}/{}'.format(wb, timestamp, modifier, quote(url))

    kwargs = {}
    status = capture['status']
    if status == '404' or status == '-':
        # '-' is a revisit; it will 404 if the underlying record is a 404
        # (also has 'mime': 'warc/revisit')
        kwargs['allow404'] = True

    resp = myrequests_get(wb_url, **kwargs)

    return construct_warcio_record(url, *fake_wb_warc(wb_url, resp, capture))


def fetch_warc_record(capture, warc_prefix):
    for field in ('url', 'filename', 'offset', 'length'):
        if field not in capture:
            raise ValueError('capture must contain '+field)

    url = capture['url']
    filename = capture['filename']
    offset = int(capture['offset'])
    length = int(capture['length'])

    warc_url = warc_prefix + '/' + filename
    headers = {'Range': 'bytes={}-{}'.format(offset, offset+length-1)}

    resp = myrequests_get(warc_url, headers=headers)
    record_bytes = resp.content

    # WARC digests can be represented in multiple ways (rfc 3548)
    # I have code in a pullreq for warcio that does this comparison
    #if 'digest' in capture and capture['digest'] != hashlib.sha1(content_bytes).hexdigest():
    #    LOGGER.error('downloaded content failed digest check')

    if record_bytes[:2] == b'\x1f\x8b':
        # warc records are either not compressed or gzip, as of 1.0
        record_bytes = gzip.decompress(record_bytes)

    count = record_bytes.count(b'\r\n\r\n')
    if count < 3:
        raise ValueError('Invalid warc response record seen')

    warcheader, httpheader, content_bytes, nothing = record_bytes.split(b'\r\n\r\n', 3)
    if len(nothing) > 0:
        raise ValueError('Invalid warc response record seen (extra bytes)')

    warcheader += b'\r\nWARC-Source-URI: ' + warc_url.encode()
    warcheader += b'\r\nWARC-Source-Range: ' + 'bytes={}-{}'.format(offset, offset+length-1).encode()

    return construct_warcio_record(url, warcheader, httpheader, content_bytes)


def construct_warcio_record(url, warcheader, httpheader, content_bytes):
    # payload will be parsed for http headers
    payload = httpheader.rstrip(b'\r\n') + b'\r\n\r\n' + content_bytes

    warc_headers_dict = {}
    if warcheader:
        for header in warcheader.split(b'\r\n')[1:]:  # skip the initial WARC/1 line
            k, v = header.split(b':', 1)
            warc_headers_dict[k] = v.strip()

    writer = WARCWriter(None)
    return writer.create_warc_record(url, 'response',
                                     payload=BytesIO(payload),
                                     warc_headers_dict=warc_headers_dict)


class CDXToolkitWARCWriter:
    def __init__(self, prefix, subprefix, info, warc_size=1000000000, gzip=True):
        self.prefix = prefix
        self.subprefix = subprefix
        self.info = info
        self.warc_size = warc_size
        self.gzip = gzip
        self.segment = 0
        self.writer = None

    def write_record(self, *args, **kwargs):
        if self.writer is None:
            self._start_new_warc()

        self.writer.write_record(*args, **kwargs)

        fsize = os.fstat(self.fd.fileno()).st_size
        if fsize > self.warc_size:
            self.fd.close()
            self.writer = None
            self.segment += 1

    def _unique_warc_filename(self):
        while True:
            name = self.prefix + '-'
            if self.subprefix is not None:
                name += self.subprefix + '-'
            name += '{:06d}'.format(self.segment) + '.extracted.warc'
            if self.gzip:
                name += '.gz'
            if os.path.exists(name):
                self.segment += 1
            else:
                break
        return name

    def _start_new_warc(self):
        self.filename = self._unique_warc_filename()
        self.fd = open(self.filename, 'wb')
        self.writer = WARCWriter(self.fd, gzip=self.gzip)
        warcinfo = self.writer.create_warcinfo_record(self.filename, self.info)
        self.writer.write_record(warcinfo)


def get_writer(prefix, subprefix, info, **kwargs):
    return CDXToolkitWARCWriter(prefix, subprefix, info, **kwargs)
