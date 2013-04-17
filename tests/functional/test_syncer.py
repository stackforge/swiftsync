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

# ENV:
# User used to synchronize both swift must own the ResellerAdmin role
# in each keystone
#
# TODO(fbo):
# SetUp must setup connector for filler and syncer
# Each test must configure its environement according to test case

import eventlet
import unittest

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
        (self.o_admin_auth_url, self.o_admin_token) = sclient.Connection(
                            self.o_st,
                            "%s:%s" % (self.o_admin_tenant, self.o_admin_user),
                            self.o_admin_password,
                            auth_version=2).get_auth()
        # Retreive admin (ResellerAdmin) token
        (self.d_admin_auth_url, self.d_admin_token) = sclient.Connection(
                            self.d_st,
                            "%s:%s" % (self.o_admin_tenant, self.o_admin_user),
                            self.o_admin_password,
                            auth_version=2).get_auth()
        # Instanciate syncer
        self.swsync = accounts.Accounts()

    def extract_created_a_u(self, created):
        account = created.keys()[0][0]
        account_id = created.keys()[0][1]
        username = created.values()[0][0][0]
        return account, account_id, username

    def create_st_account_url(self, account_id):
        o_account_url = self.o_admin_auth_url.split('AUTH_')[0] \
                        + 'AUTH_' + account_id
        d_account_url = self.d_admin_auth_url.split('AUTH_')[0] \
                        + 'AUTH_' + account_id
        return o_account_url, d_account_url

    def verify_account_diff(self, alo, ald):
        for k, v in alo[0].items():
            if k not in ('x-timestamp', 'x-trans-id', 'date'):
                self.assertEqual(ald[0][k], v, msg='%s differs' %k)
    
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

    def test_sync_one_empty_account(self):
        """ One empty account with meta data
        """
        index = {}
        # Create account
        self.created = filler.create_swift_account(self.o_ks_client,
                                                   self.pile,
                                                   1, 1, index)
        account, account_id, username = self.extract_created_a_u(self.created)
        # Post meta data on account
        tenant_cnx = sclient.Connection(self.o_st,
                                        "%s:%s" % (account, username),
                                        self.default_user_password,
                                        auth_version=2)
        filler.create_account_meta(tenant_cnx)
        # Start sync process
        self.swsync.process()
        # Create account storage url
        o_account_url, d_account_url = self.create_st_account_url(account_id)
        # Retreive account details
        o_cnx = sclient.http_connection(o_account_url)
        d_cnx = sclient.http_connection(d_account_url)
        alo = sclient.get_account(None, self.o_admin_token,
                            http_conn=o_cnx,
                            full_listing=True)
        ald = sclient.get_account(None, self.d_admin_token,
                            http_conn=d_cnx,
                            full_listing=True)
        self.verify_account_diff(alo, ald)
    
    def test_sync_many_empty_account(self):
        """ Many empty account with meta data
        """
        pass

                                     
    def tearDown(self):
        if self.created:
            for k, v in self.created.items():
                user_info_list = [user[1] for user in v]
                account_id = k[1]
                o_account_url, d_account_url = \
                        self.create_st_account_url(account_id)
                # Remove account content on origin and destination
                self.delete_account_cont(o_account_url, self.o_admin_token)
                self.delete_account_cont(d_account_url, self.d_admin_token)
                # We just need to delete keystone accounts and users
                # in origin keystone as syncer does not sync
                # keystone database
                filler.delete_account(self.o_ks_client,
                                      user_info_list,
                                      k)
