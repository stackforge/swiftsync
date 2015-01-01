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

# To start this functional test the admin users (on both keystone) used
# to synchronize the destination swift must own the ResellerAdmin role in
# keystone.

import unittest

import eventlet
from keystoneclient.v2_0 import client as ksclient
from swiftclient import client as sclient

from swsync import accounts
from swsync import filler
from swsync.utils import get_config


class TestSyncer(unittest.TestCase):

    def setUp(self):
        self.o_st = get_config('auth', 'keystone_origin')
        self.d_st = get_config('auth', 'keystone_dest')
        self.default_user_password = get_config('filler',
                                                'default_user_password')
        # Retreive configuration for filler
        self.o_admin_tenant, self.o_admin_user, self.o_admin_password = (
            get_config('auth', 'keystone_origin_admin_credentials').split(':'))
        self.sw_c_concu = int(get_config('concurrency',
                              'filler_swift_client_concurrency'))
        self.ks_c_concu = int(get_config('concurrency',
                              'filler_keystone_client_concurrency'))
        self.pile = eventlet.GreenPile(self.sw_c_concu)
        self.pool = eventlet.GreenPool(self.ks_c_concu)
        # Set a keystone connection to origin server
        self.o_ks_client = ksclient.Client(
            auth_url=self.o_st,
            username=self.o_admin_user,
            password=self.o_admin_password,
            tenant_name=self.o_admin_tenant)
        # Set a keystone connection to destination server
        self.d_ks_client = ksclient.Client(
            auth_url=self.d_st,
            username=self.o_admin_user,
            password=self.o_admin_password,
            tenant_name=self.o_admin_tenant)
        # Retreive admin (ResellerAdmin) token
        (self.o_admin_auth_url, self.o_admin_token) = (
            sclient.Connection(self.o_st,
                               "%s:%s" % (self.o_admin_tenant,
                                          self.o_admin_user),
                               self.o_admin_password,
                               auth_version=2).get_auth())
        # Retreive admin (ResellerAdmin) token
        (self.d_admin_auth_url, self.d_admin_token) = (
            sclient.Connection(self.d_st,
                               "%s:%s" % (self.o_admin_tenant,
                                          self.o_admin_user),
                               self.o_admin_password,
                               auth_version=2).get_auth())
        # Instanciate syncer
        self.swsync = accounts.Accounts()

    def extract_created_a_u_iter(self, created):
        for ad, usd in created.items():
            account = ad[0]
            account_id = ad[1]
            # Retreive the first user as we only need one
            username = usd[0][0]
            yield account, account_id, username

    def create_st_account_url(self, account_id):
        o_account_url = (
            self.o_admin_auth_url.split('AUTH_')[0] + 'AUTH_' + account_id)
        d_account_url = (
            self.d_admin_auth_url.split('AUTH_')[0] + 'AUTH_' + account_id)
        return o_account_url, d_account_url

    def verify_aco_diff(self, alo, ald):
        # Verify account, container, object diff in HEAD struct
        for k, v in alo[0].items():
            if k not in ('x-timestamp', 'x-trans-id',
                         'date', 'last-modified'):
                self.assertEqual(ald[0][k], v, msg='%s differs' % k)

    def delete_account_cont(self, account_url, token):
        cnx = sclient.http_connection(account_url)
        al = sclient.get_account(None, token,
                                 http_conn=cnx,
                                 full_listing=True)
        for container in [c['name'] for c in al[1]]:
            ci = sclient.get_container(None, token,
                                       container, http_conn=cnx,
                                       full_listing=True)
            on = [od['name'] for od in ci[1]]
            for obj in on:
                sclient.delete_object('', token, container,
                                      obj, http_conn=cnx)
            sclient.delete_container('', token, container, http_conn=cnx)

    def get_url(self, account_id, s_type):
        # Create account storage url
        o_account_url, d_account_url = self.create_st_account_url(account_id)
        if s_type == 'orig':
            url = o_account_url
        elif s_type == 'dest':
            url = d_account_url
        else:
            raise Exception('Unknown type')
        return url

    def get_cnx(self, account_id, s_type):
        url = self.get_url(account_id, s_type)
        return sclient.http_connection(url)

    def get_account_detail(self, account_id, token, s_type):
        cnx = self.get_cnx(account_id, s_type)
        return sclient.get_account(None, token,
                                   http_conn=cnx,
                                   full_listing=True)

    def list_containers(self, account_id, token, s_type):
        cd = self.get_account_detail(account_id, token, s_type)
        return cd[1]

    def get_container_detail(self, account_id, token, s_type, container):
        cnx = self.get_cnx(account_id, s_type)
        return sclient.get_container(None, token, container,
                                     http_conn=cnx, full_listing=True)

    def list_objects(self, account_id, token, s_type, container):
        cd = self.get_container_detail(account_id, token, s_type, container)
        return cd[1]

    def list_objects_in_containers(self, account_id, token, s_type):
        ret = {}
        cl = self.list_containers(account_id, token, s_type)
        for c in [c['name'] for c in cl]:
            objs = self.list_objects(account_id, token, s_type, c)
            ret[c] = objs
        return ret

    def get_object_detail(self, account_id, token, s_type, container, obj):
        cnx = self.get_cnx(account_id, s_type)
        return sclient.get_object("", token, container, obj, http_conn=cnx)

    def get_account_meta(self, account_id, token, s_type):
        d = self.get_account_detail(account_id, token, s_type)
        return {k: v for k, v in d[0].iteritems()
                if k.startswith('x-account-meta')}

    def get_container_meta(self, account_id, token, s_type, container):
        d = self.get_container_detail(account_id, token, s_type, container)
        return {k: v for k, v in d[0].iteritems()
                if k.startswith('x-container-meta')}

    def post_account(self, account_id, token, s_type, headers):
        cnx = self.get_cnx(account_id, s_type)
        sclient.post_account("", token, headers, http_conn=cnx)

    def post_container(self, account_id, token, s_type, container, headers):
        cnx = self.get_cnx(account_id, s_type)
        sclient.post_container("", token, container, headers, http_conn=cnx)

    def put_container(self, account_id, token, s_type, container):
        cnx = self.get_cnx(account_id, s_type)
        sclient.put_container("", token, container, http_conn=cnx)

    def delete_container(self, account_id, token, s_type, container):
        cnx = self.get_cnx(account_id, s_type)
        sclient.delete_container("", token, container, http_conn=cnx)

    def post_object(self, account_id, token, s_type, container, name, headers):
        cnx = self.get_cnx(account_id, s_type)
        sclient.post_object("", token, container, name, headers, http_conn=cnx)

    def put_object(self, account_id, token, s_type, container, name, content):
        cnx = self.get_cnx(account_id, s_type)
        sclient.put_object("", token, container, name, content, http_conn=cnx)

    def delete_object(self, account_id, token, s_type,
                      container, name):
        cnx = self.get_cnx(account_id, s_type)
        sclient.delete_object("", token, container, name,
                              http_conn=cnx)

    def test_01_sync_one_empty_account(self):
        """One empty account with meta data."""
        index = {}
        # create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   1, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # post meta data on account
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            filler.create_account_meta(tenant_cnx)

        # start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            alo = self.get_account_detail(account_id,
                                          self.o_admin_token, 'orig')
            ald = self.get_account_detail(account_id,
                                          self.d_admin_token, 'dest')
            self.verify_aco_diff(alo, ald)

    def test_02_sync_many_empty_account(self):
        """Many empty account with meta data."""
        index = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   3, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # Post meta data on account
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            filler.create_account_meta(tenant_cnx)

        # Start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            alo = self.get_account_detail(account_id,
                                          self.o_admin_token, 'orig')
            ald = self.get_account_detail(account_id,
                                          self.d_admin_token, 'dest')
            self.verify_aco_diff(alo, ald)

    def test_03_sync_many_accounts_with_many_containers_meta(self):
        """Many accounts with many containers and container meta data."""
        index = {}
        index_container = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   3, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            acc = (account, account_id)
            filler.create_containers(tenant_cnx, acc, 3, index_container)

        # Start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # Verify container listing
            clo = self.list_containers(account_id,
                                       self.o_admin_token, 'orig')
            cld = self.list_containers(account_id,
                                       self.d_admin_token, 'dest')
            self.assertEqual(len(clo), len(cld))
            for do in clo:
                match = [dd for dd in cld if dd['name'] == do['name']]
                self.assertEqual(len(match), 1)
                self.assertDictEqual(do, match[0])
            # Verify container details
            clo_c_names = [d['name'] for d in clo]
            for c_name in clo_c_names:
                cdo = self.get_container_detail(account_id, self.o_admin_token,
                                                'orig', c_name)
                cdd = self.get_container_detail(account_id, self.d_admin_token,
                                                'dest', c_name)
            self.verify_aco_diff(cdo, cdd)

    def test_04_sync_many_accounts_many_containers_and_obj_meta(self):
        """Many accounts with many containers and some object."""
        index = {}
        index_container = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   1, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            acc = (account, account_id)
            filler.create_containers(tenant_cnx, acc, 1, index_container)
            filler.create_objects(tenant_cnx, acc, 1, 2048, index_container)

        # Start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # Verify container listing
            olo = self.list_objects_in_containers(account_id,
                                                  self.o_admin_token, 'orig')
            old = self.list_objects_in_containers(account_id,
                                                  self.d_admin_token, 'dest')

            # Verify we have the same amount of container
            self.assertListEqual(olo.keys(), old.keys())
            # For each container
            for c, objs in olo.items():
                for obj in objs:
                    # Verify first object detail returned by container
                    # server
                    match = [od for od in old[c] if od['name'] == obj['name']]
                    self.assertEqual(len(match), 1)
                    obj_d = match[0]
                    self.assertDictEqual(obj, obj_d)
                # Verify object details from object server
                obj_names = [d['name'] for d in olo[c]]
                for obj_name in obj_names:
                    objd_o = self.get_object_detail(account_id,
                                                    self.o_admin_token, 'orig',
                                                    c, obj_name)
                    objd_d = self.get_object_detail(account_id,
                                                    self.d_admin_token, 'dest',
                                                    c, obj_name)
                    self.verify_aco_diff(objd_o, objd_d)
                    # Verify content
                    self.assertEqual(objd_o[1], objd_d[1])

    def test_05_account_two_passes(self):
        """Account modified two sync passes."""
        index = {}
        # create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   3, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # post meta data on account
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            filler.create_account_meta(tenant_cnx)

        # start sync process
        self.swsync.process()

        # Add more meta to account
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # Modify meta data on account
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            token = tenant_cnx.get_auth()[1]
            # Remove one, modify one, and add one meta
            a_meta = self.get_account_meta(account_id,
                                           token,
                                           'orig')
            a_meta_k_names = [k.split('-')[-1] for k in a_meta]
            headers = {}
            headers['X-Account-Meta-a1'] = 'b1'
            headers["X-Remove-Account-Meta-%s" % a_meta_k_names[0]] = 'x'
            headers["X-Account-Meta-%s" % a_meta_k_names[1]] = 'b2'
            self.post_account(account_id, token,
                              'orig', headers=headers)

        # Re - start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            alo = self.get_account_detail(account_id,
                                          self.o_admin_token, 'orig')
            ald = self.get_account_detail(account_id,
                                          self.d_admin_token, 'dest')
            self.verify_aco_diff(alo, ald)

    def test_06_container_two_passes(self):
        """Containers modified two sync passes."""
        index = {}
        index_container = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   3, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            acc = (account, account_id)
            filler.create_containers(tenant_cnx, acc, 3, index_container)

        # Start sync process
        self.swsync.process()

        # Modify container in account
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)

            token = tenant_cnx.get_auth()[1]
            # Modify an existing container meta
            clo = self.list_containers(account_id,
                                       token, 'orig')
            co_name = clo[0]['name']
            c_meta = self.get_container_meta(account_id,
                                             token, 'orig',
                                             co_name)
            c_meta_k_names = [k.split('-')[-1] for k in c_meta]
            headers = {}
            headers['X-Container-Meta-a1'] = 'b1'
            headers["X-Remove-Container-Meta-%s" % c_meta_k_names[0]] = 'x'
            headers["X-Container-Meta-%s" % c_meta_k_names[1]] = 'b2'
            self.post_container(account_id, token,
                                'orig',
                                headers=headers,
                                container=co_name)
            # Add a some more container
            self.put_container(account_id, token, 'orig', 'foobar')
            self.put_container(account_id, token, 'orig', 'foobar1')
            self.put_container(account_id, token, 'orig', 'foobar2')
            # Delete one container
            co_name = clo[1]['name']
            self.delete_container(account_id, token, 'orig', co_name)

        # Re - Start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # Verify container listing
            clo = self.list_containers(account_id,
                                       self.o_admin_token, 'orig')
            cld = self.list_containers(account_id,
                                       self.d_admin_token, 'dest')
            self.assertEqual(len(clo), len(cld))
            for do in clo:
                match = [dd for dd in cld if dd['name'] == do['name']]
                self.assertEqual(len(match), 1)
                self.assertDictEqual(do, match[0])
            # Verify container details
            clo_c_names = [d['name'] for d in clo]
            for c_name in clo_c_names:
                cdo = self.get_container_detail(account_id, self.o_admin_token,
                                                'orig', c_name)
                cdd = self.get_container_detail(account_id, self.d_admin_token,
                                                'dest', c_name)
                self.verify_aco_diff(cdo, cdd)

    def test_07_object_two_passes(self):
        """Objects modified two passes."""
        index = {}
        index_container = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   1, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            acc = (account, account_id)
            filler.create_containers(tenant_cnx, acc, 1, index_container)
            filler.create_objects(tenant_cnx, acc, 3, 2048, index_container)

        # Start sync process
        self.swsync.process()

        # Modify objects in containers
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)

            token = tenant_cnx.get_auth()[1]
            c_o = self.list_objects_in_containers(account_id, token, 'orig')
            for cont, objs in c_o.iteritems():
                for obj in objs:
                    # Modify object meta
                    obj_d, data = self.get_object_detail(account_id,
                                                         token, 'orig',
                                                         cont, obj['name'])
                    meta = {k: v for k, v in obj_d.iteritems()
                            if k.startswith('x-object-meta')}
                    meta_k_names = [k.split('-')[-1] for k in meta]
                    headers = {}
                    headers['X-Object-Meta-a1'] = 'b1'
                    headers["X-Remove-Object-Meta-%s" % meta_k_names[0]] = 'x'
                    headers["X-Object-Meta-%s" % meta_k_names[1]] = 'b2'
                    self.post_object(account_id, token,
                                     'orig',
                                     headers=headers,
                                     container=cont,
                                     name=obj['name'])
                # Create an object
                self.put_object(account_id, token, 'orig',
                                cont, 'foofoo', 'barbarbar')
                self.put_object(account_id, token, 'orig',
                                cont, 'foofoo1', 'barbarbar')
                self.put_object(account_id, token, 'orig',
                                cont, 'foofoo2', 'barbarbar')

                o_names = [o['name'] for o in objs]
                # Delete an object
                name = o_names[0]
                self.delete_object(account_id, token, 'orig',
                                   cont, name)

        # Start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            # Verify container listing
            olo = self.list_objects_in_containers(account_id,
                                                  self.o_admin_token, 'orig')
            old = self.list_objects_in_containers(account_id,
                                                  self.d_admin_token, 'dest')

            # Verify we have the same amount of container
            self.assertListEqual(olo.keys(), old.keys())
            # For each container
            for c, objs in olo.items():
                for obj in objs:
                    # Verify first object detail returned by container server
                    match = [od for od in old[c] if od['name'] == obj['name']]
                    self.assertEqual(len(match), 1)
                    obj_d = match[0]
                    a = obj.copy()
                    b = obj_d.copy()
                    del a['last_modified']
                    del b['last_modified']
                    self.assertDictEqual(a, b)
                # Verify object details from object server
                obj_names = [d['name'] for d in olo[c]]
                for obj_name in obj_names:
                    objd_o = self.get_object_detail(account_id,
                                                    self.o_admin_token, 'orig',
                                                    c, obj_name)
                    objd_d = self.get_object_detail(account_id,
                                                    self.d_admin_token, 'dest',
                                                    c, obj_name)
                    self.verify_aco_diff(objd_o, objd_d)
                    # Verify content
                    self.assertEqual(objd_o[1], objd_d[1])

    def test_08_sync_containers_with_last_modified(self):
        """Containers with last-modified middleware."""
        index = {}
        index_container = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   1, 1, index)

        # Create container and store new account && container
        account_dest, container_dest = None, None
        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            acc = (account, account_id)
            filler.create_containers(tenant_cnx, acc, 1, index_container)
            filler.create_objects(tenant_cnx, acc, 1, 2048, index_container)
            cld = self.list_containers(account_id,
                                       self.d_admin_token, 'orig')
            account_dest = account_id
            container_dest = cld[0]['name']
            break

        # Start sync process
        self.swsync.process()

        # Update dest
        self.put_object(account_dest, self.d_admin_token, 'dest',
                        container_dest, 'lm-test', 'lm-data')

        # Get timestamp
        cdd = self.get_container_detail(account_dest, self.d_admin_token,
                                        'dest', container_dest)
        try:
            dest_lm = cdd[0]['x-container-meta-last-modified']
        except(KeyError):
            # Last-modified middleware is not present
            return

        # Restart sync process
        self.swsync.process()

        # Check if dest timestamp have not been updated
        cdd = self.get_container_detail(account_dest, self.d_admin_token,
                                        'dest', container_dest)
        self.assertEqual(dest_lm, cdd[0]['x-container-meta-last-modified'])

        # Check if new object is still present in dest
        obj_detail = self.get_object_detail(account_dest, self.d_admin_token,
                                            'dest', container_dest, 'lm-test')
        self.assertEqual('lm-data', obj_detail[1])

    def tearDown(self):
        if self.created:
            for k, v in self.created.items():
                user_info_list = [user[1] for user in v]
                account_id = k[1]
                o_account_url, d_account_url = (
                    self.create_st_account_url(account_id))
                # Remove account content on origin and destination
                self.delete_account_cont(o_account_url, self.o_admin_token)
                self.delete_account_cont(d_account_url, self.d_admin_token)
                # We just need to delete keystone accounts and users
                # in origin keystone as syncer does not sync
                # keystone database
                filler.delete_account(self.o_ks_client,
                                      user_info_list,
                                      k)
