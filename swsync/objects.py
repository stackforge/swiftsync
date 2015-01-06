# -*- coding: utf-8 -*-
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Chmouel Boudjnah <chmouel@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import logging
import urllib
import urllib2

import eventlet
import swift.common.bufferedhttp
import swift.common.http
try:
    from swift.container.sync import _Iter2FileLikeObject as FileLikeIter
except ImportError:
    # Nov2013: swift.common.utils now include a more generic object
    from swift.common.utils import FileLikeIter
from swiftclient import client as swiftclient


def quote(value, safe='/'):
    """Patched version of urllib.quote.

    Encodes utf-8 strings before quoting.
    """
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    return urllib.quote(value, safe)


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
    with eventlet.Timeout(conn_timeout):
        conn = swift.common.bufferedhttp.http_connect_raw(
            x.hostname,
            x.port,
            'GET',
            path,
            headers=headers,
            ssl=False)

    with eventlet.Timeout(response_timeout):
        resp = conn.getresponse()

    if not swift.common.http.is_success(resp.status):
        resp.read()
        # TODO(chmou): logging
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


def delete_object(dest_cnx,
                  dest_token,
                  container_name,
                  object_name):
    parsed = dest_cnx[0]
    url = '%s://%s/%s' % (parsed.scheme, parsed.netloc, parsed.path)
    swiftclient.delete_object(url=url,
                              token=dest_token,
                              container=container_name,
                              http_conn=dest_cnx,
                              name=object_name)


def sync_object(orig_storage_url, orig_token, dest_storage_url,
                dest_token, container_name, object_name_etag):
    object_name = object_name_etag[1]

    orig_headers, orig_body = get_object(orig_storage_url,
                                         orig_token,
                                         container_name,
                                         object_name)
    container_name = quote(container_name)

    post_headers = orig_headers
    post_headers['x-auth-token'] = dest_token
    sync_to = dest_storage_url + "/" + container_name
    try:
        swiftclient.put_object(sync_to, name=object_name,
                               headers=post_headers,
                               contents=FileLikeIter(orig_body))
    except(swiftclient.ClientException), e:
        logging.info("error sync object: %s, %s" % (
                     object_name, e.http_reason))
