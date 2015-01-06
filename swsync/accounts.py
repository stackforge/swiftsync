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
import datetime
import logging
import os
import time

import dateutil.relativedelta
import keystoneclient.v2_0.client
import swiftclient

import swsync.containers
from swsync.utils import ConfigurationError
from swsync.utils import get_config


class Accounts(object):
    """Process Keystone Accounts."""
    def __init__(self):
        self.keystone_cnx = None
        self.container_cls = swsync.containers.Containers()

    def get_swift_auth(self, auth_url, tenant, user, password):
        """Get swift connexion from args."""
        return swiftclient.client.Connection(
            auth_url,
            '%s:%s' % (tenant, user),
            password,
            auth_version=2).get_auth()

    def get_ks_auth_orig(self):
        """Get keystone cnx from config."""
        orig_auth_url = get_config('auth', 'keystone_origin')
        cfg = get_config('auth', 'keystone_origin_admin_credentials')
        (tenant_name, username, password) = cfg.split(':')

        return keystoneclient.v2_0.client.Client(auth_url=orig_auth_url,
                                                 username=username,
                                                 password=password,
                                                 tenant_name=tenant_name)

    def get_target_tenant_filter(self):
        """Returns a set of target tenants from the tenant_list_file.

        tenant_list_file is defined in the config file or given as a command
        line argument.

        If tenant_list_file is not defined, returns None (an empty filter).

        """
        try:
            tenant_filter_filename = get_config('sync', 'tenant_filter_file')

            with open(tenant_filter_filename) as tenantsfile:
                return {name.strip() for name in tenantsfile.readlines()}
        except ConfigurationError:
            return None

    def account_headers_clean(self, account_headers, to_null=False):
        ret = {}
        for key, value in account_headers.iteritems():
            if key.startswith('x-account-meta'):
                if to_null:
                    value = ''
                ret[key] = value
        return ret

    def sync_account(self, orig_storage_url, orig_token,
                     dest_storage_url, dest_token):
        """Sync a single account with url/tok to dest_url/dest_tok."""
        orig_storage_cnx = swiftclient.http_connection(orig_storage_url)
        dest_storage_cnx = swiftclient.http_connection(dest_storage_url)
        account_id = os.path.basename(orig_storage_url.replace("AUTH_", ''))

        try:
            orig_account_headers, orig_containers = (
                swiftclient.get_account(None, orig_token,
                                        http_conn=orig_storage_cnx,
                                        full_listing=True))

            dest_account_headers, dest_containers = (
                swiftclient.get_account(None, dest_token,
                                        http_conn=dest_storage_cnx,
                                        full_listing=True))
        except(swiftclient.client.ClientException), e:
                logging.info("error getting account: %s, %s" % (
                    account_id, e.http_reason))
                return

        self.container_cls.delete_container(dest_storage_cnx,
                                            dest_token,
                                            orig_containers,
                                            dest_containers)

        do_headers = False
        if len(dest_account_headers) != len(orig_account_headers):
            do_headers = True
        else:
            for k, v in orig_account_headers.iteritems():
                if not k.startswith('x-account-meta'):
                    continue
                if k not in dest_account_headers:
                    do_headers = True
                elif dest_account_headers[k] != v:
                    do_headers = True

        if do_headers:
            orig_metadata_headers = self.account_headers_clean(
                orig_account_headers)
            dest_metadata_headers = self.account_headers_clean(
                dest_account_headers, to_null=True)

            new_headers = dict(dest_metadata_headers.items() +
                               orig_metadata_headers.items())
            try:
                swiftclient.post_account(
                    "", dest_token, new_headers,
                    http_conn=dest_storage_cnx,
                )
                logging.info("HEADER: sync headers: %s" % (account_id))
            except(swiftclient.client.ClientException), e:
                logging.info("ERROR: updating container metadata: %s, %s" % (
                    account_id, e.http_reason))
                # We don't pass on because since the server was busy
                # let's pass it on for the next pass
                return

        container_list = iter(orig_containers)
        if swsync.utils.REVERSE:
            container_list = reversed(orig_containers)

        for container in container_list:
            logging.info("Syncronizing container %s: %s",
                         container['name'], container)
            dt1 = datetime.datetime.fromtimestamp(time.time())
            self.container_cls.sync(orig_storage_cnx,
                                    orig_storage_url,
                                    orig_token,
                                    dest_storage_cnx,
                                    dest_storage_url, dest_token,
                                    container['name'])

            dt2 = datetime.datetime.fromtimestamp(time.time())
            rd = dateutil.relativedelta.relativedelta(dt2, dt1)
            # TODO(chmou): use logging
            logging.info("%s done: %d hours, %d minutes and %d seconds",
                         container['name'],
                         rd.hours,
                         rd.minutes, rd.seconds)

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

        # if user has defined target tenants, limit the migration
        # to them
        _targets_filters = self.get_target_tenant_filter()
        if _targets_filters is not None:
            _targets = (tenant for tenant in self.keystone_cnx.tenants.list()
                        if tenant.name in _targets_filters)
        else:
            _targets = self.keystone_cnx.tenants.list()

        for tenant in _targets:
            user_orig_st_url = bare_oa_st_url + tenant.id
            user_dst_st_url = bare_dst_st_url + tenant.id

            self.sync_account(user_orig_st_url,
                              orig_admin_token,
                              user_dst_st_url,
                              dest_admin_token)


def main():
    acc = Accounts()
    acc.process()
