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
import time
import datetime

import swiftclient
import dateutil.relativedelta
import keystoneclient.v2_0.client

from utils import get_config
import swsync.containers


class Accounts(object):
    """Process Keystone Accounts."""
    def __init__(self):
        self.keystone_cnx = None
        self.container_cls = swsync.containers.Containers()

    def get_swift_auth(self, auth_url, tenant, user, password):
        """Get swift connexion from args"""
        return swiftclient.client.Connection(
            auth_url,
            '%s:%s' % (tenant, user),
            password,
            auth_version=2).get_auth()

    def get_ks_auth_orig(self):
        """Get keystone cnx from config"""
        orig_auth_url = get_config('auth', 'keystone_origin')
        cfg = get_config('auth', 'keystone_origin_admin_credentials')
        (tenant_name, username, password) = cfg.split(':')

        return keystoneclient.v2_0.client.Client(auth_url=orig_auth_url,
                                                 username=username,
                                                 password=password,
                                                 tenant_name=tenant_name)

    def sync_account(self, orig_storage_url, orig_token,
                     dest_storage_url, dest_token):
        """Sync a single account with url/tok to dest_url/dest_tok."""
        orig_storage_cnx = swiftclient.http_connection(orig_storage_url)
        dest_storage_cnx = swiftclient.http_connection(dest_storage_url)

        _, orig_containers = (
            swiftclient.get_account(None, orig_token,
                                    http_conn=orig_storage_cnx,
                                    full_listing=True))

        for container in orig_containers:
            print container
            dt1 = datetime.datetime.fromtimestamp(time.time())
            self.container_cls.sync(orig_storage_cnx,
                                    orig_storage_url,
                                    orig_token,
                                    dest_storage_cnx,
                                    dest_storage_url, dest_token,
                                    container['name'])

            dt2 = datetime.datetime.fromtimestamp(time.time())
            rd = dateutil.relativedelta.relativedelta(dt2, dt1)
            #TODO(chmou): use logging
            print "%d hours, %d minutes and %d seconds" % (rd.hours,
                                                           rd.minutes,
                                                           rd.seconds)
            print

    def process(self):
        """Process all keystone accounts to sync."""
        orig_auth_url = get_config('auth', 'keystone_origin')
        orig_admin_tenant, orig_admin_user, orig_admin_password = (
            get_config('auth', 'keystone_origin_admin_credentials').split(':'))
        oa_st_url, orig_admin_token = self.get_swift_auth(
            orig_auth_url, orig_admin_tenant,
            orig_admin_user, orig_admin_password)
        dest_auth_url = get_config('auth', 'keystone_dest')

        # we assume orig and dest passwd are the same obv synchronized.
        dst_st_url, dest_admin_token = self.get_swift_auth(
            dest_auth_url, orig_admin_tenant,
            orig_admin_user, orig_admin_password)

        bare_oa_st_url = oa_st_url[:oa_st_url.find('AUTH_')] + "AUTH_"
        bare_dst_st_url = dst_st_url[:dst_st_url.find('AUTH_')] + "AUTH_"

        self.keystone_cnx = self.get_ks_auth_orig()

        for tenant in self.keystone_cnx.tenants.list():
            user_orig_st_url = bare_oa_st_url + tenant.id
            user_dst_st_url = bare_dst_st_url + tenant.id

            self.sync_account(user_orig_st_url,
                              orig_admin_token,
                              user_dst_st_url,
                              dest_admin_token)


def main():
    acc = Accounts()
    acc.process()
