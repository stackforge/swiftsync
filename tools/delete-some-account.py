#!/usr/bin/env python
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
import argparse
import sys
import time

import keystoneclient.v2_0.client
import swiftclient

import swsync.utils

MAX_RETRIES = 5


def main():
    """Delete some accounts."""
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument('-d', action='store_true',
                        dest="dest",
                        help='Check destination')
    parser.add_argument('number', nargs=1, type=int)

    args = parser.parse_args()

    print "Are you sure you want to delete? Control-C if you don't"
    time.sleep(5)
    number = args.number[0]

    credentials = swsync.utils.get_config('auth',
                                          'keystone_origin_admin_credentials')
    (tenant_name, username, password) = credentials.split(':')
    if args.dest:
        auth_url = swsync.utils.get_config('auth', 'keystone_dest')
    else:
        auth_url = swsync.utils.get_config('auth', 'keystone_origin')
    keystone_cnx = keystoneclient.v2_0.client.Client(auth_url=auth_url,
                                                     username=username,
                                                     password=password,
                                                     tenant_name=tenant_name)

    storage_url, admin_token = swiftclient.client.Connection(
        auth_url, '%s:%s' % (tenant_name, username), password,
        auth_version=2).get_auth()
    bare_storage_url = storage_url[:storage_url.find('AUTH_')] + "AUTH_"

    TENANT_LIST = keystone_cnx.tenants.list()
    mid = int(len(TENANT_LIST) / 2)
    for tenant in TENANT_LIST[mid:mid + number]:
        tenant_storage_url = bare_storage_url + tenant.id
        swiftcnx = swiftclient.client.Connection(preauthurl=tenant_storage_url,
                                                 preauthtoken=admin_token,
                                                 retries=MAX_RETRIES)
        _, containers = swiftcnx.get_account()
        for cont in containers:
            _, objects = swiftcnx.get_container(cont['name'])
            print "deleting %s" % (cont['name'])
            for obj in objects:
                print "deleting %s/%s" % (cont['name'], obj['name'])
                swiftcnx.delete_object(cont['name'], obj['name'])
            swiftcnx.delete_container(cont['name'])

if __name__ == '__main__':
    main()
