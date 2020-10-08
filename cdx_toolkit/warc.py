from urllib.parse import quote
from io import BytesIO
import os.path
import datetime
import logging
import sys

from warcio import WARCWriter
from warcio.recordloader import ArcWarcRecordLoader
from warcio.bufferedreaders import DecompressingBufferedReader
from warcio.statusandheaders import StatusAndHeaders

from .myrequests import myrequests_get
from .timeutils import http_date_to_datetime, datetime_to_iso_date

LOGGER = logging.getLogger(__name__)


def wb_redir_to_original(location):
    return 'http' + location.split('_/http', 1)[1]


http_status_text = {
    300: 'Multiple Choices',
    301: 'Moved Permanently',
    302: 'Found',
    303: 'See Other',
    304: 'Not Modified',
    307: 'Temporary Redirect',
    308: 'Permanent Redirect',
}


def fake_wb_warc(url, wb_url, resp, capture):
    '''
    Given a playback from a wayback, fake up a warc response record
    '''
    status_code = resp.status_code
    status_reason = resp.reason

    if str(status_code) != capture['status']:
        url = capture['url']
        timestamp = capture['timestamp']
        if status_code == 200 and capture['status'] == '-':
            LOGGER.warning('revisit record vivified by wayback for %s %s',
                           url, timestamp)
        elif status_code == 200 and capture['status'].startswith('3'):
            LOGGER.warning('redirect capture came back 200, same-surt same-timestamp capture? %s %s',
                           url, timestamp)
        elif status_code == 302 and capture['status'].startswith('3'):
            # this is OK, wayback always sends a temporary redir
            status_code = int(capture['status'])
            if status_code != resp.status_code and status_code in http_status_text:
                status_reason = http_status_text[status_code]
        else:  # pragma: no cover
            LOGGER.warning('surprised that status code is now=%d orig=%s %s %s',
                           status_code, capture['status'], url, timestamp)

    http_headers = []
    http_date = None
    for k, v in resp.headers.items():
        kl = k.lower()
        if kl.startswith('x-archive-orig-date'):
            http_date = v

        if kl.startswith('x-archive-orig-'):
            k = k[len('x-archive-orig-'):]
            http_headers.append((k, v))
        elif kl == 'content-type':
            http_headers.append(('Content-Type', v))
        elif kl == 'location':
            v = wb_redir_to_original(v)
            http_headers.append((k, v))
        else:
            if not kl.startswith('x-archive-'):
                k = 'X-Archive-' + k
            http_headers.append((k, v))

    statusline = '{} {}'.format(status_code, status_reason)
    http_headers = StatusAndHeaders(statusline, headers=http_headers, protocol='HTTP/1.1')

    warc_headers_dict = {
        'WARC-Source-URI': wb_url,
        'WARC-Creation-Date': datetime_to_iso_date(datetime.datetime.now()),
    }
    if http_date:
        warc_headers_dict['WARC-Date'] = datetime_to_iso_date(http_date_to_datetime(http_date))

    content_bytes = resp.content

    writer = WARCWriter(None)  # needs warc_version here?
    return writer.create_warc_record(url, 'response',
                                     payload=BytesIO(content_bytes),
                                     http_headers=http_headers,
                                     warc_headers_dict=warc_headers_dict)


def fetch_wb_warc(capture, wb, modifier='id_'):
    for field in ('url', 'timestamp', 'status'):
        if field not in capture:  # pragma: no cover
            raise ValueError('capture must contain '+field)

    if wb is None:  # pragma: no cover
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

    return fake_wb_warc(url, wb_url, resp, capture)


def fetch_warc_record(capture, warc_url_prefix):
    for field in ('url', 'filename', 'offset', 'length'):
        if field not in capture:  # pragma: no cover
            raise ValueError('capture must contain '+field)

    url = capture['url']
    filename = capture['filename']
    offset = int(capture['offset'])
    length = int(capture['length'])

    warc_url = warc_url_prefix + '/' + filename
    headers = {'Range': 'bytes={}-{}'.format(offset, offset+length-1)}

    resp = myrequests_get(warc_url, headers=headers)
    record_bytes = resp.content
    stream = DecompressingBufferedReader(BytesIO(record_bytes))
    record = ArcWarcRecordLoader().parse_record_stream(stream)

    for header in ('WARC-Source-URI', 'WARC-Source-Range'):
        if record.rec_headers.get_header(header):  # pragma: no cover
            print('Surprised that {} was already set in this WARC record'.format(header), file=sys.stderr)

    warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')
    if url != warc_target_uri:  # pragma: no cover
        print('Surprised that WARC-Target-URI {} is not the capture url {}'.format(warc_target_uri, url), file=sys.stderr)

    record.rec_headers.replace_header('WARC-Source-URI', warc_url)
    record.rec_headers.replace_header('WARC-Source-Range', 'bytes={}-{}'.format(offset, offset+length-1))
    return record


class CDXToolkitWARCWriter:
    def __init__(self, prefix, subprefix, info, size=1000000000, gzip=True, warc_version=None):
        self.prefix = prefix
        self.subprefix = subprefix
        self.info = info
        self.size = size
        self.gzip = gzip
        self.warc_version = warc_version
        self.segment = 0
        self.writer = None

    def write_record(self, *args, **kwargs):
        if self.writer is None:
            if self.warc_version is None:
                # opportunity to intuit warc version here
                self.warc_version = '1.0'
            if self.warc_version != '1.0':
                LOGGER.error('WARC versions other than 1.0 are not correctly supported yet')
                # ...because fake_wb_warc always generates 1.0
            # should we also check the warcinfo record to make sure it's got a matching warc_version inside?
            self._start_new_warc()

        self.writer.write_record(*args, **kwargs)

        fsize = os.fstat(self.fd.fileno()).st_size
        if fsize > self.size:
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
        LOGGER.info('opening new warc file %s', self.filename)
        self.writer = WARCWriter(self.fd, gzip=self.gzip, warc_version=self.warc_version)
        warcinfo = self.writer.create_warcinfo_record(self.filename, self.info)
        self.writer.write_record(warcinfo)


def get_writer(prefix, subprefix, info, **kwargs):
    return CDXToolkitWARCWriter(prefix, subprefix, info, **kwargs)
