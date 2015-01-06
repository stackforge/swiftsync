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


class TestAccountBase(tests.units.base.TestCase):
    def setUp(self):
        super(TestAccountBase, self).setUp()
        self.accounts_cls = swsync.accounts.Accounts()
        self._stubs()

    def _stubs(self):
        self.stubs.Set(keystoneclient.v2_0, 'client', fakes.FakeKS)
        self.stubs.Set(swiftclient.client, 'Connection',
                       fakes.FakeSWConnection)
        self.stubs.Set(swsync.accounts, 'get_config', fakes.fake_get_config)
        self.stubs.Set(swsync.accounts.Accounts, 'get_target_tenant_filter',
                       fakes.fake_get_filter)
        self.stubs.Set(swiftclient, 'http_connection',
                       fakes.FakeSWClient.http_connection)


class TestAccountSyncMetadata(TestAccountBase):
    def _base_sync_metadata(self, orig_dict={},
                            dest_dict={},
                            get_account_called=[],
                            post_account_called=[],
                            info_called=[],
                            sync_container_called=[],
                            raise_post_account=False):

        def fake_info(msg, *args):
            info_called.append(msg)
        self.stubs.Set(logging, 'info', fake_info)

        def get_account(self, *args, **kwargs):
            if len(get_account_called) == 0:
                get_account_called.append(args)
                return orig_dict
            else:
                get_account_called.append(args)
                return dest_dict
        self.stubs.Set(swiftclient, 'get_account', get_account)

        def post_account(url, token, headers, **kwargs):
            post_account_called.append(headers)

            if raise_post_account:
                raise swiftclient.client.ClientException("Error in testing")
        self.stubs.Set(swiftclient, 'post_account', post_account)

        class Containers(object):
            def sync(*args, **kwargs):
                sync_container_called.append(args)

            def delete_container(*args, **kwargs):
                pass

        self.accounts_cls.container_cls = Containers()
        self.accounts_cls.sync_account("http://orig", "otoken",
                                       "http://dest", "dtoken")

    def test_sync_metadata_delete_dest(self):
        get_account_called = []
        sync_container_called = []
        post_account_called = []
        info_called = []

        orig_dict = ({'x-account-meta-life': 'beautiful',
                      'x-account-container-count': 1},
                     [{'name': 'cont1'}])

        dest_dict = ({'x-account-meta-vita': 'bella',
                      'x-account-container-count': 1},
                     [{'name': 'cont1'}])
        self._base_sync_metadata(orig_dict,
                                 dest_dict,
                                 info_called=info_called,
                                 sync_container_called=sync_container_called,
                                 post_account_called=post_account_called,
                                 get_account_called=get_account_called)

        self.assertEqual(len(sync_container_called), 1)
        self.assertEqual(len(get_account_called), 2)
        self.assertTrue(info_called)

        self.assertIn('x-account-meta-life',
                      post_account_called[0])
        self.assertEqual(post_account_called[0]['x-account-meta-life'],
                         'beautiful')
        self.assertIn('x-account-meta-vita',
                      post_account_called[0])
        self.assertEqual(post_account_called[0]['x-account-meta-vita'],
                         '')

    def test_sync_metadata_update_dest(self):
        get_account_called = []
        sync_container_called = []
        post_account_called = []
        info_called = []

        orig_dict = ({'x-account-meta-life': 'beautiful',
                      'x-account-container-count': 1},
                     [{'name': 'cont1'}])

        dest_dict = ({'x-account-meta-life': 'bella',
                      'x-account-container-count': 1},
                     [{'name': 'cont1'}])
        self._base_sync_metadata(orig_dict,
                                 dest_dict,
                                 info_called=info_called,
                                 sync_container_called=sync_container_called,
                                 post_account_called=post_account_called,
                                 get_account_called=get_account_called)

        self.assertEqual(len(sync_container_called), 1)
        self.assertEqual(len(get_account_called), 2)
        self.assertTrue(info_called)

        self.assertIn('x-account-meta-life',
                      post_account_called[0])
        self.assertEqual(post_account_called[0]['x-account-meta-life'],
                         'beautiful')

        self.assertIn('x-account-meta-life',
                      post_account_called[0])
        self.assertEqual(post_account_called[0]['x-account-meta-life'],
                         'beautiful')

    def test_sync_metadata_add_to_dest(self):
        info_called = []
        get_account_called = []
        sync_container_called = []
        post_account_called = []

        orig_dict = ({'x-account-meta-life': 'beautiful',
                      'x-account-container-count': 1},
                     [{'name': 'cont1'}])

        dest_dict = ({'x-account-container-count': 1},
                     [{'name': 'cont1'}])
        self._base_sync_metadata(orig_dict,
                                 dest_dict,
                                 info_called=info_called,
                                 sync_container_called=sync_container_called,
                                 post_account_called=post_account_called,
                                 get_account_called=get_account_called)

        self.assertEqual(len(sync_container_called), 1)
        self.assertEqual(len(get_account_called), 2)
        self.assertTrue(info_called)

        self.assertIn('x-account-meta-life',
                      post_account_called[0])
        self.assertEqual(post_account_called[0]['x-account-meta-life'],
                         'beautiful')

        self.assertIn('x-account-meta-life',
                      post_account_called[0])
        self.assertEqual(post_account_called[0]['x-account-meta-life'],
                         'beautiful')

    def test_sync_metadata_raise(self):
        info_called = []
        get_account_called = []
        sync_container_called = []
        post_account_called = []

        orig_dict = ({'x-account-meta-life': 'beautiful',
                      'x-account-container-count': 1},
                     [{'name': 'cont1'}])

        dest_dict = ({'x-account-container-count': 1},
                     [{'name': 'cont1'}])
        self._base_sync_metadata(orig_dict,
                                 dest_dict,
                                 info_called=info_called,
                                 sync_container_called=sync_container_called,
                                 post_account_called=post_account_called,
                                 get_account_called=get_account_called,
                                 raise_post_account=True)
        self.assertTrue(info_called)
        self.assertIn('ERROR: updating container metadata: orig, ',
                      info_called)
        self.assertFalse(sync_container_called)


class TestAccountSync(TestAccountBase):
    def test_get_swift_auth(self):
        tenant_name = 'foo1'
        ret = self.accounts_cls.get_swift_auth(
            "http://test.com", tenant_name, "user", "password")
        tenant_id = fakes.TENANTS_LIST[tenant_name]['id']
        self.assertEqual(ret[0], "%s/v1/AUTH_%s" % (fakes.STORAGE_DEST,
                                                    tenant_id))

    def test_get_ks_auth_orig(self):
        _, kwargs = self.accounts_cls.get_ks_auth_orig()()
        k = fakes.CONFIGDICT['auth']['keystone_origin_admin_credentials']
        tenant_name, username, password = k.split(':')

        self.assertEqual(kwargs['tenant_name'], tenant_name)
        self.assertEqual(kwargs['username'], username)
        self.assertEqual(kwargs['password'], password)
        k = fakes.CONFIGDICT['auth']['keystone_origin']
        self.assertEqual(k, kwargs['auth_url'])

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
        self.assertEqual(tenant_list_ids, ret_orig_storage_id)
        [self.assertTrue(y[1].startswith(fakes.STORAGE_DEST)) for y in ret]

    def test_sync_account(self):
        ret = []

        def get_account(*args, **kwargs):
            return ({'x-account-container-count': len(fakes.CONTAINERS_LIST)},
                    [x[0] for x in fakes.CONTAINERS_LIST])
        self.stubs.Set(swiftclient, 'get_account', get_account)

        class Containers(object):
            def sync(*args, **kwargs):
                ret.append(args)

            def delete_container(*args, **kwargs):
                pass
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
        self.assertEqual(ret_container_list, default_container_list)

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
            # ORIG
            if len(ret) == 0:
                ret.append("TESTED")
                return ({'x-account-container-count': 1},
                        [{'name': 'foo'}])
            # DEST
            else:
                return ({'x-account-container-count': 2},
                        [{'name': 'foo', 'name': 'bar'}])

            raise swiftclient.client.ClientException("TESTED")
        self.stubs.Set(swiftclient, 'get_account', get_account)
        self.accounts_cls.sync_account("http://foo", "token",
                                       "http://bar", "token2")
        self.assertTrue(called)
