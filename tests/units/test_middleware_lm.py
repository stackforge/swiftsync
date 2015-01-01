# -*- encoding: utf-8 -*-

# Copyright 2013 eNovance.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Author : "Fabien Boucher <fabien.boucher@enovance.com>"
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import unittest

from middlewares import last_modified as middleware

import swift.common.swob as swob


class FakeApp(object):
    def __init__(self, status_headers_body=None):
        self.status_headers_body = status_headers_body
        if not self.status_headers_body:
            self.status_headers_body = ('204 No Content', {}, '')

    def __call__(self, env, start_response):
        status, headers, body = self.status_headers_body
        return swob.Response(status=status, headers=headers,
                             body=body)(env, start_response)


class FakeRequest(object):
    def get_response(self, app):
        pass


class TestLastModifiedMiddleware(unittest.TestCase):

    def _make_request(self, path, **kwargs):
        req = swob.Request.blank("/v1/AUTH_account/%s" % path, **kwargs)
        return req

    def setUp(self):
        self.conf = {'key_name': 'Last-Modified'}
        self.test_default = middleware.filter_factory(self.conf)(FakeApp())

    def test_denied_method_conf(self):
        app = FakeApp()
        test = middleware.filter_factory({})(app)
        self.assertEqual(test.key_name, 'Last-Modified')
        test = middleware.filter_factory({'key_name': "Last Modified"})(app)
        self.assertEqual(test.key_name, 'Last-Modified')
        test = middleware.filter_factory({'key_name': "Custom Key"})(app)
        self.assertEqual(test.key_name, 'Custom-Key')

    def test_PUT_on_container(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, True)

    def test_POST_on_container(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont',
                                 environ={'REQUEST_METHOD': 'POST'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, True)

    def test_DELETE_on_container(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, False)

    def test_GET_on_container_and_object(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont',
                                 environ={'REQUEST_METHOD': 'GET'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, False)
        self.called = False
        req = self._make_request('cont/obj',
                                 environ={'REQUEST_METHOD': 'GET'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, False)

    def test_POST_on_object(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont/obj',
                                 environ={'REQUEST_METHOD': 'POST'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, True)

    def test_PUT_on_object(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont/obj',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, True)

    def test_DELETE_on_object(self):
        self.called = False

        def make_pre_authed_request(*args, **kargs):
            self.called = True
            return FakeRequest()

        middleware.wsgi.make_pre_authed_request = make_pre_authed_request
        req = self._make_request('cont/obj',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.get_response(self.test_default)
        self.assertEqual(self.called, True)
