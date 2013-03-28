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
import swiftclient
from eventlet import sleep, Timeout

import base as test_base
import swsync.objects as swobjects
from fakes import STORAGE_ORIG, STORAGE_DEST, TENANTS_LIST


def fake_http_connect(status, body='', headers={}, resp_waitfor=None,
                      connect_waitfor=None):
    class FakeConn(object):
        def __init__(self, status):
            self.reason = 'PSG'
            self.status = status
            self.body = body
            if connect_waitfor:
                sleep(int(connect_waitfor))

        def getheaders(self):
            return headers

        def getresponse(self):
            if resp_waitfor:
                sleep(int(resp_waitfor))
            return self

        def read(self, amt=None):
            rv = self.body[:amt]
            self.body = self.body[amt:]
            return rv

    def connect(*args, **kwargs):
        return FakeConn(status)
    return connect


class TestObject(test_base.TestCase):
    def setUp(self):
        super(TestObject, self).setUp()
        self.tenant_name = 'foo1'
        self.tenant_id = TENANTS_LIST[self.tenant_name]['id']
        self.orig_storage_url = "%s/AUTH_%s" % (STORAGE_ORIG, self.tenant_id)
        self.dest_storage_url = "%s/AUTH_%s" % (STORAGE_DEST, self.tenant_id)

    def test_quote(self):
        utf8_chars = u'\uF10F\uD20D\uB30B\u9409\u8508\u5605\u3703\u1801'
        try:
            swobjects.quote(utf8_chars)
        except(KeyError):
            self.fail("utf8 was not properly quoted")

    def test_get_object_not_found(self):
        new_connect = fake_http_connect(404)
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)

        self.assertRaises(swiftclient.ClientException,
                          swobjects.get_object,
                          self.orig_storage_url, "token", "cont1", "obj1")

    def test_sync_object(self):
        body = ("X" * 3) * 1024
        new_connect = fake_http_connect(200, body)
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)

        def put_object(url, name=None, headers=None, contents=None):
            self.assertEqual('obj1', name)
            self.assertIn('x-auth-token', headers)
            self.assertIsInstance(contents, swobjects._Iter2FileLikeObject)
            contents_read = contents.read()
            self.assertEqual(len(contents_read), len(body))

        self.stubs.Set(swobjects.swiftclient, 'put_object', put_object)

        swobjects.sync_object(self.orig_storage_url,
                              "token", self.dest_storage_url, "token",
                              "cont1", ("etag", "obj1"))

    def test_get_object_chunked(self):
        chunk_size = 32
        expected_chunk_time = 3
        body = ("X" * expected_chunk_time) * chunk_size

        new_connect = fake_http_connect(200, body)
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)

        headers, gen = swobjects.get_object(self.orig_storage_url,
                                            "token", "cont1", "obj1",
                                            resp_chunk_size=chunk_size)
        sent_time = 0
        for chunk in gen:
            sent_time += 1
        self.assertEqual(sent_time, expected_chunk_time)

    def test_get_object_full(self):
        new_connect = fake_http_connect(200, body='foobar')
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)

        headers, body = swobjects.get_object(self.orig_storage_url,
                                             "token", "cont1", "obj1",
                                             resp_chunk_size=None)
        self.assertEqual(body, 'foobar')

    def test_get_headers(self):
        headers = {'X-FOO': 'BaR'}.items()
        new_connect = fake_http_connect(200, headers=headers)
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)

        headers, gen = swobjects.get_object(self.orig_storage_url,
                                            "token",
                                            "cont1",
                                            "obj1")
        self.assertIn('x-foo', headers)
        self.assertEquals(headers['x-foo'], 'BaR')

    def test_get_object_over_conn_timeout(self):
        new_connect = fake_http_connect(200, connect_waitfor=2)
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)
        self.assertRaises(Timeout,
                          swobjects.get_object,
                          self.orig_storage_url, "token", "cont1", "obj1",
                          conn_timeout=1)

    def test_get_object_over_resp_timeout(self):
        new_connect = fake_http_connect(200, resp_waitfor=2)
        self.stubs.Set(swobjects, 'http_connect_raw', new_connect)
        self.assertRaises(Timeout,
                          swobjects.get_object,
                          self.orig_storage_url, "token", "cont1", "obj1",
                          response_timeout=1)
