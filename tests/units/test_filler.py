# -*- coding: utf-8 -*-
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
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

import eventlet
from fakes import FakeKSClient
from fakes import FakeKSTenant
from fakes import FakeKSUser
from fakes import FakeSWConnection
from keystoneclient.exceptions import ClientException as KSClientException
import swiftclient

from swsync import filler
from swsync import utils
from tests.units import base


class TestFiller(base.TestCase):
    def setUp(self):
        super(TestFiller, self).setUp()
        self._stubs()

    def _stubs(self):
        self.stubs.Set(swiftclient.client, 'Connection',
                       FakeSWConnection)

    def get_connection(self, *args):
        return swiftclient.client.Connection(utils.get_config(
                                             'auth', 'keystone_origin'),
                                             'test', 'password',
                                             tenant_name='test')

    def test_create_containers(self):
        get_containers_created = []
        return_dict_ref = {}

        def put_container(*args, **kwargs):
            get_containers_created.append(args[1])

        self.stubs.Set(FakeSWConnection, 'put_container', put_container)
        cnx = self.get_connection()
        filler.create_containers(cnx, 'test', 3, return_dict_ref)
        self.assertEqual(len(get_containers_created), 3)
        self.assertEqual(get_containers_created[0].split('_')[0],
                         'container')
        meta_amount = len(return_dict_ref['test'].values())
        self.assertEqual(meta_amount, 3)

    def test_create_containers_fail(self):
        get_containers_created = []
        return_dict_ref = {}
        self.attempts = 0

        def put_container(*args, **kwargs):
            if self.attempts == 0:
                self.attempts += 1
                raise swiftclient.client.ClientException('Fake err msg')
            else:
                self.attempts += 1
                get_containers_created.append(args[1])

        self.stubs.Set(FakeSWConnection, 'put_container', put_container)
        cnx = self.get_connection()
        filler.create_containers(cnx, 'test', 3, return_dict_ref)

        self.assertEqual(len(get_containers_created), 2)

    def test_create_objects(self):
        get_object_created = []
        return_dict_ref = {'test': {'container_a': {'objects': []},
                                    'container_b': {'objects': []}}}

        def put_object(*args, **kwargs):
            get_object_created.append(args[1:])

        self.stubs.Set(FakeSWConnection,
                       'put_object',
                       put_object)
        cnx = self.get_connection()
        filler.create_objects(cnx, 'test', 2, 2048, return_dict_ref)
        objects_ca = return_dict_ref['test']['container_a']['objects']
        objects_cb = return_dict_ref['test']['container_b']['objects']
        self.assertEqual(len(objects_ca), 2)
        self.assertEqual(len(objects_cb), 2)

    def test_create_objects_fail(self):
        get_object_created = []
        return_dict_ref = {'test': {'container_a': {'objects': []}}}
        self.attempts = 0

        def put_object(*args, **kwargs):
            if self.attempts == 0:
                self.attempts += 1
                raise swiftclient.client.ClientException('Fake err msg')
            else:
                self.attempts += 1
                get_object_created.append(args[1:])

        self.stubs.Set(FakeSWConnection,
                       'put_object',
                       put_object)
        cnx = self.get_connection()
        filler.create_objects(cnx, 'test', 2, 2048, return_dict_ref)
        objects_ca = return_dict_ref['test']['container_a']['objects']
        self.assertEqual(len(objects_ca), 1)

    def test_fill_swift(self):
        self.cont_cnt = 0
        self.obj_cnt = 0
        return_dict_ref = {}

        def create_objects(*args, **kwargs):
            self.obj_cnt += 1

        def create_containers(*args, **kwargs):
            self.cont_cnt += 1

        def swift_cnx(*args, **kargs):
            return self.get_connection()

        self.stubs.Set(filler, 'swift_cnx', swift_cnx)
        self.stubs.Set(filler, 'create_objects', create_objects)
        self.stubs.Set(filler, 'create_containers', create_containers)

        concurrency = int(utils.get_config('concurrency',
                                           'filler_swift_client_concurrency'))
        pool = eventlet.GreenPool(concurrency)

        created = {('account1', 'account1_id'): ['test', 'test_id', 'role_id'],
                   ('account2', 'account2_id'): ['test', 'test_id', 'role_id']}
        filler.fill_swift(pool, created, 1, 1, 2048, return_dict_ref)
        self.assertEqual(self.cont_cnt, 2)
        self.assertEqual(self.obj_cnt, 2)

    def test_create_swift_user(self):
        self.create_cnt = 0
        self.role_cnt = 0

        def create(*args, **kargs):
            self.create_cnt += 1
            return FakeKSUser()

        def add_user_role(*args, **kargs):
            self.role_cnt += 1

        co = utils.get_config('auth',
                              'keystone_origin_admin_credentials').split(':')
        tenant_name, username, password = co
        client = FakeKSClient()
        client.roles.add_user_role = add_user_role
        client.users.create = create
        filler.create_swift_user(client, 'account1', 'account1_id', 1)

        self.assertEqual(self.create_cnt, 1)
        self.assertEqual(self.role_cnt, 1)

    def test_create_swift_user_fail(self):
        self.pa = 0

        def create(*args, **kargs):
            if self.pa == 0:
                self.pa += 1
                raise KSClientException('Fake msg')
            else:
                self.pa += 1
                return FakeKSUser()

        def add_user_role(*args, **kargs):
            pass

        co = utils.get_config('auth',
                              'keystone_origin_admin_credentials').split(':')
        tenant_name, username, password = co
        client = FakeKSClient()
        client.roles.add_user_role = add_user_role
        client.users.create = create
        users = filler.create_swift_user(client, 'account1', 'account1_id', 3)

        self.assertEqual(len(users), 2)

    def test_create_swift_account(self):
        self.ret_index = {}
        self.user_cnt = 0

        def create_swift_user(*args):
            self.user_cnt += 1

        self.stubs.Set(filler, 'create_swift_user', create_swift_user)

        concurrency = int(utils.get_config('concurrency',
                          'filler_keystone_client_concurrency'))
        pile = eventlet.GreenPile(concurrency)
        client = FakeKSClient()
        filler.create_swift_account(client, pile, 1, 1, self.ret_index)

        self.assertEqual(self.user_cnt, 1)
        self.assertEqual(len(self.ret_index.keys()), 1)

    def test_create_swift_account_fail(self):
        self.ret_index = {}
        self.pa = 0

        def create_tenant(*args):
            if self.pa == 0:
                self.pa += 1
                raise KSClientException('Fake msg')
            else:
                self.pa += 1
                return FakeKSTenant('foo1')

        def create_swift_user(*args):
            pass

        client = FakeKSClient()

        self.stubs.Set(client.tenants, 'create', create_tenant)
        self.stubs.Set(filler, 'create_swift_user', create_swift_user)

        concurrency = int(utils.get_config('concurrency',
                          'filler_keystone_client_concurrency'))
        pile = eventlet.GreenPile(concurrency)
        filler.create_swift_account(client, pile, 3, 1, self.ret_index)

        self.assertEqual(len(self.ret_index.keys()), 2)

    def test_delete_account(self):
        self.delete_t_cnt = 0
        self.delete_u_cnt = 0

        def delete_t(*args):
            self.delete_t_cnt += 1

        def delete_u(*args):
            self.delete_u_cnt += 1

        client = FakeKSClient()
        client.tenants.delete = delete_t
        client.users.delete = delete_u
        filler.delete_account(client,
                              [FakeKSUser().id, ],
                              ('account1', 'account1_id'))

        self.assertEqual(self.delete_t_cnt, 1)
        self.assertEqual(self.delete_u_cnt, 1)

    def test_delete_account_content(self):
        self.cnt_ga = 0
        self.cnt_co = 0
        self.cnt_do = 0

        filler.swift_cnx = self.get_connection

        def get_account(*args, **kwargs):
            self.cnt_ga += 1
            return (None, ({'name': 'cont1'}, {'name': 'cont2'}))

        def get_container(*args, **kwargs):
            self.cnt_co += 1
            return (None, ({'name': 'obj1'}, {'name': 'obj2'}))

        def delete_object(*args, **kwargs):
            self.cnt_do += 1

        self.stubs.Set(FakeSWConnection, 'get_account', get_account)
        self.stubs.Set(FakeSWConnection, 'get_container', get_container)
        self.stubs.Set(FakeSWConnection, 'delete_object', delete_object)

        filler.delete_account_content('account1', ['user', 'user_id'])

        self.assertEqual(self.cnt_ga, 1)
        self.assertEqual(self.cnt_co, 2)
        self.assertEqual(self.cnt_do, 4)
