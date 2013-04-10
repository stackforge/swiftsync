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

import keystoneclient
import swiftclient

import swsync.accounts
import tests.units.base
import tests.units.fakes as fakes


class TestAccount(tests.units.base.TestCase):
    def setUp(self):
        super(TestAccount, self).setUp()
        self.accounts_cls = swsync.accounts.Accounts()
        self._stubs()

    def get_account(self, *args, **kwargs):
        return ({'x-account-container-count': len(fakes.CONTAINERS_LIST)},
                [x[0] for x in fakes.CONTAINERS_LIST])

    def _stubs(self):
        self.stubs.Set(keystoneclient.v2_0, 'client', fakes.FakeKS)
        self.stubs.Set(swiftclient.client, 'Connection',
                       fakes.FakeSWConnection)
        self.stubs.Set(swsync.accounts, 'get_config', fakes.fake_get_config)
        self.stubs.Set(swiftclient, 'get_account', self.get_account)
        self.stubs.Set(swiftclient, 'http_connection',
                       fakes.FakeSWClient.http_connection)

    def test_get_swift_auth(self):
        tenant_name = 'foo1'
        ret = self.accounts_cls.get_swift_auth(
            "http://test.com", tenant_name, "user", "password")
        tenant_id = fakes.TENANTS_LIST[tenant_name]['id']
        self.assertEquals(ret[0], "%s/v1/AUTH_%s" % (fakes.STORAGE_DEST,
                                                     tenant_id))

    def test_get_ks_auth_orig(self):
        _, kwargs = self.accounts_cls.get_ks_auth_orig()()
        k = fakes.CONFIGDICT['auth']['keystone_origin_admin_credentials']
        tenant_name, username, password = k.split(':')

        self.assertEquals(kwargs['tenant_name'], tenant_name)
        self.assertEquals(kwargs['username'], username)
        self.assertEquals(kwargs['password'], password)
        k = fakes.CONFIGDICT['auth']['keystone_origin']
        self.assertEquals(k, kwargs['auth_url'])

    def test_process(self):
        ret = []

        def sync_account(orig_storage_url,
                         orig_token,
                         dest_storage_url,
                         dest_token):
            ret.append((orig_storage_url, dest_storage_url))
        self.accounts_cls.sync_account = sync_account
        self.accounts_cls.process()
        tenant_list_ids = sorted(fakes.TENANTS_LIST[x]['id']
                                 for x in fakes.TENANTS_LIST)
        ret_orig_storage_id = sorted(
            x[0][x[0].find('AUTH_') + 5:] for x in ret)
        self.assertEquals(tenant_list_ids, ret_orig_storage_id)
        [self.assertTrue(y[1].startswith(fakes.STORAGE_DEST)) for y in ret]

    def test_sync_account(self):
        ret = []

        class Containers(object):
            def sync(*args, **kwargs):
                ret.append(args)
        self.accounts_cls.container_cls = Containers()

        tenant_name = fakes.TENANTS_LIST.keys()[0]
        tenant_id = fakes.TENANTS_LIST[tenant_name]['id']
        orig_storage_url = "%s/AUTH_%s" % (fakes.STORAGE_ORIG,
                                           tenant_id)
        dest_storage_url = "%s/AUTH_%s" % (fakes.STORAGE_DEST,
                                           tenant_id)
        self.accounts_cls.sync_account(orig_storage_url, "otoken",
                                       dest_storage_url, "dtoken")
        ret_container_list = sorted(x[7] for x in ret)
        default_container_list = sorted(x[0]['name']
                                        for x in fakes.CONTAINERS_LIST)
        self.assertEquals(ret_container_list, default_container_list)

    def test_sync_exception_get_account(self):
        called = []

        def fake_info(self, *args):
            called.append("called")

        def get_account(*args, **kwargs):
            raise swiftclient.client.ClientException("TESTED")
        self.stubs.Set(swiftclient, 'get_account', get_account)
        self.stubs.Set(logging, 'info', fake_info)
        self.accounts_cls.sync_account("http://foo", "token",
                                       "http://bar", "token2")
        self.assertTrue(called)

    def test_sync_account_detect_we_need_to_delete_some_stuff(self):
        # I should get my lazy ass up and just use self.mox stuff
        ret = []
        called = []

        class Containers():
            def delete_container(*args, **kwargs):
                called.append("TESTED")

            def sync(*args, **kwargs):
                pass

        self.accounts_cls.container_cls = Containers()

        def get_account(*args, **kwargs):
            #ORIG
            if len(ret) == 0:
                ret.append("TESTED")
                return ({'x-account-container-count': 1},
                        [{'name': 'foo'}])
            #DEST
            else:
                return ({'x-account-container-count': 2},
                        [{'name': 'foo', 'name': 'bar'}])

            raise swiftclient.client.ClientException("TESTED")
        self.stubs.Set(swiftclient, 'get_account', get_account)
        self.accounts_cls.sync_account("http://foo", "token",
                                       "http://bar", "token2")
        self.assertTrue(called)
