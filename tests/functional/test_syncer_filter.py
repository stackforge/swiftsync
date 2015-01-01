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
#    Author : "Joe Hakim Rahme <joe.hakim.rahme@enovance.com>"
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

# To start this functional test the admin users (on both keystone) used
# to synchronize the destination swift must own the ResellerAdmin role in
# keystone.
#
# In your config.ini file, you should uncomment the field tenant_filter_file
# and specify a path to a file where you're allowed to read and write.

import random
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
        self.filter_filename = get_config('sync', 'tenant_filter_file')
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
        """Verify that 2 accounts are similar to validate migration."""
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

    def test_01_sync_one_of_two_empty_accounts(self):
        """Create two empty accounts, Sync only one."""
        index = {}

        # create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   2, 1, index)

        for account, account_id, username in (
                self.extract_created_a_u_iter(self.created)):

            # post meta data on account
            tenant_cnx = sclient.Connection(self.o_st,
                                            "%s:%s" % (account, username),
                                            self.default_user_password,
                                            auth_version=2)
            filler.create_account_meta(tenant_cnx)

        # select random account and write it in the filter file
        t_account, t_account_id, t_username = random.choice(list(
            self.extract_created_a_u_iter(self.created)))
        with open(self.filter_filename, "w") as filterlist:
            filterlist.write(t_account + "\n")

        # start sync process
        self.swsync.process()

        # Now verify dest
        for account, account_id, username in (
            self.extract_created_a_u_iter(self.created)):
                alo = self.get_account_detail(account_id,
                                          self.o_admin_token, 'orig')
                ald = self.get_account_detail(account_id,
                                          self.d_admin_token, 'dest')
                if account == t_account:
                    self.verify_aco_diff(alo, ald)

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
