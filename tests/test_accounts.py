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
import keystoneclient

import base as test_base
import sync.accounts
from fakes import FakeSWConnection, TENANTS_LIST, STORAGE_ORIG, \
    STORAGE_DEST, FakeSWClient, FakeKS, CONFIGDICT, CONTAINERS_LIST, \
    fake_get_config


class TestAccount(test_base.TestCase):
    def setUp(self):
        super(TestAccount, self).setUp()
        self.accounts_cls = sync.accounts.Accounts()
        self._stubs()

    def _stubs(self):
        self.stubs.Set(keystoneclient.v2_0, 'client', FakeKS)
        self.stubs.Set(swiftclient.client, 'Connection', FakeSWConnection)
        self.stubs.Set(sync.accounts, 'get_config', fake_get_config)
        self.stubs.Set(swiftclient, 'get_account', FakeSWClient.get_account)
        self.stubs.Set(swiftclient, 'http_connection',
                       FakeSWClient.http_connection)

    def test_get_swift_auth(self):
        tenant_name = 'foo1'
        ret = self.accounts_cls.get_swift_auth(
            "http://test.com", tenant_name, "user", "password")
        tenant_id = TENANTS_LIST[tenant_name]['id']
        self.assertEquals(ret[0], "%s/v1/AUTH_%s" % (STORAGE_DEST,
                                                     tenant_id))

    def test_get_ks_auth_orig(self):
        _, kwargs = self.accounts_cls.get_ks_auth_orig()()
        k = CONFIGDICT['auth']['keystone_origin_admin_credentials']
        tenant_name, username, password = k.split(':')

        self.assertEquals(kwargs['tenant_name'], tenant_name)
        self.assertEquals(kwargs['username'], username)
        self.assertEquals(kwargs['password'], password)
        k = CONFIGDICT['auth']['keystone_origin']
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
        tenant_list_ids = sorted(TENANTS_LIST[x]['id'] for x in TENANTS_LIST)
        ret_orig_storage_id = sorted(
            x[0][x[0].find('AUTH_') + 5:] for x in ret)
        self.assertEquals(tenant_list_ids, ret_orig_storage_id)
        [self.assertTrue(x[1].startswith(STORAGE_DEST)) for x in ret]

    def test_sync_account(self):
        ret = []

        class Containers(object):
            def sync(*args, **kwargs):
                ret.append(args)

        sync.accounts.containers.Containers = Containers

        tenant_name = TENANTS_LIST.keys()[0]
        orig_storage_url = "%s/AUTH_%s" % (STORAGE_ORIG,
                                           TENANTS_LIST[tenant_name]['id'])
        dest_storage_url = "%s/AUTH_%s" % (STORAGE_DEST,
                                           TENANTS_LIST[tenant_name]['id'])
        self.accounts_cls.sync_account(orig_storage_url, "otoken",
                                       dest_storage_url, "dtoken")
        ret_container_list = sorted(x[7] for x in ret)
        default_container_list = sorted(x[0]['name'] for x in CONTAINERS_LIST)
        self.assertEquals(ret_container_list, default_container_list)
