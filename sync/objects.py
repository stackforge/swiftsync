# -*- encoding: utf-8 -*-
from swiftclient import client as swiftclient
from swift.common.bufferedhttp import http_connect_raw
from swift.common.http import is_success
from swift.container.sync import _Iter2FileLikeObject
from eventlet import Timeout
import urllib2
from urllib import quote as urllib_quote


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf-8 strings before quoting
    """
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    return urllib_quote(value, safe)


def get_object(storage_url, token,
               container_name,
               object_name,
               response_timeout=15,
               conn_timeout=5,
               resp_chunk_size=65536):
    headers = {'x-auth-token': token}
    x = urllib2.urlparse.urlparse(storage_url)

    path = x.path + '/' + container_name + '/' + object_name
    path = quote(path)
    with Timeout(conn_timeout):
        conn = http_connect_raw(
            x.hostname,
            x.port,
            'GET',
            path,
            headers=headers,
            ssl=False)

    with Timeout(response_timeout):
        resp = conn.getresponse()

    if not is_success(resp.status):
        resp.read()
        raise swiftclient.ClientException(
            'status %s %s' % (resp.status, resp.reason))

    if resp_chunk_size:
        def _object_body():
            buf = resp.read(resp_chunk_size)
            while buf:
                yield buf
                buf = resp.read(resp_chunk_size)
        object_body = _object_body()
    else:
        object_body = resp.read()

    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value

    return (resp_headers, object_body)


def sync_object(orig_storage_url, orig_token, dest_storage_url,
                dest_token, container_name, object_name_etag):
    orig_headers, orig_body = get_object(orig_storage_url,
                                         orig_token,
                                         container_name,
                                         object_name_etag[1],
                                         )
    container_name = quote(container_name)
    object_name = quote(object_name_etag[1])

    post_headers = orig_headers
    post_headers['x-auth-token'] = dest_token
    sync_to = dest_storage_url + "/" + container_name
    swiftclient.put_object(sync_to, name=object_name,
                           headers=post_headers,
                           contents=_Iter2FileLikeObject(orig_body))

if __name__ == '__main__':
    pass
