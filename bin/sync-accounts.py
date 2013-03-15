#!/usr/bin/python
# -*- encoding: utf-8 -*-
import swiftclient

from sync.containers import sync_container
from utils import get_config, get_auth

orig_auth_url = get_config('auth', 'keystone_origin')
orig_tenant, orig_user, orig_password = \
    get_config('auth', 'keystone_origin_credentials').split(':')

dest_auth_url = get_config('auth', 'keystone_dest')
dest_tenant, dest_user, dest_password = \
    get_config('auth', 'keystone_dest_credentials').split(':')

orig_storage_url, orig_token = \
    get_auth(orig_auth_url, orig_tenant, orig_user, orig_password)
orig_storage_cnx = swiftclient.http_connection(orig_storage_url)

dest_storage_url, dest_token = \
    get_auth(dest_auth_url, dest_tenant, dest_user, dest_password)
dest_storage_cnx = swiftclient.http_connection(dest_storage_url)


orig_account_stats, orig_containers = swiftclient.get_account(
    None, orig_token, http_conn=orig_storage_cnx, full_listing=True
)

for container in orig_containers:
    print "Synching %s.." % (container)
    sync_container(orig_storage_cnx, orig_storage_url, orig_token,
                   dest_storage_cnx, dest_storage_url, dest_token,
                   container['name'])
