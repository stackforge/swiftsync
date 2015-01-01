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
#
#   Last-Modified middleware must be installed in the proxy-server
#   pipeline.

import unittest

import swiftclient

CONF = {
    'user': ('demo:demo', 'wxcvbn'),
    'auth_url': 'http://192.168.56.101:5000/v2.0',
}


class TestLastModifiedMiddleware(unittest.TestCase):

    def setUp(self):
        self.user = swiftclient.client.Connection(
            CONF['auth_url'],
            CONF['user'][0],
            CONF['user'][1],
            auth_version='2.0',
        )
        self.container_name = 'container'
        self.meta = 'x-container-meta-last-modified'

        self.user.put_container(self.container_name)

    def _verify_meta(self, exist=True):
        cont_d = self.user.get_container(self.container_name)
        if exist:
            self.assertTrue(self.meta in cont_d[0].keys())
            epoch = cont_d[0][self.meta]
            self.assertTrue(float(epoch) > 1)
        else:
            self.assertFalse(self.meta in cont_d[0].keys())

    def _get_meta(self):
        cont_d = self.user.get_container(self.container_name)
        return float(cont_d[0][self.meta])

    def test_POST_container(self):
        self.user.post_container(self.container_name, {'key': 'val'})
        self._verify_meta()

    def test_multiple_POST_container(self):
        self.user.post_container(self.container_name, {'key': 'val'})
        epoch1 = self._get_meta()
        self.user.post_container(self.container_name, {'key': 'val'})
        epoch2 = self._get_meta()
        self.assertNotEqual(epoch1, epoch2)

    def test_GET_container(self):
        self.user.get_container(self.container_name)
        self._verify_meta(exist=False)

    def test_PUT_object(self):
        self.user.put_object(self.container_name, 'obj_name', 'content')
        self._verify_meta()

    def test_multiple_PUT_object(self):
        self.user.put_object(self.container_name, 'obj_name', 'content')
        epoch1 = self._get_meta()
        self.user.put_object(self.container_name, 'obj_name2', 'content')
        epoch2 = self._get_meta()
        self.assertNotEqual(epoch1, epoch2)

    def test_DELETE_object(self):
        self.user.put_object(self.container_name, 'obj_name', 'content')
        epoch1 = self._get_meta()
        self.user.delete_object(self.container_name, 'obj_name')
        epoch2 = self._get_meta()
        self.assertNotEqual(epoch1, epoch2)

    def tearDown(self):
        # Verify and delete container content
        cont_d = self.user.get_container(self.container_name)
        for obj in cont_d[1]:
            self.user.delete_object(self.container_name, obj['name'])
        self.user.delete_container(self.container_name)
