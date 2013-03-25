# -*- encoding: utf-8 -*-
import datetime
import os
import sys
import time

from keystoneclient.v2_0 import client as ksclient
import dateutil.relativedelta
import swiftclient

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.utils import get_config, get_auth
from sync.containers import sync_container


def get_ks_auth_orig():
    orig_auth_url = get_config('auth', 'keystone_origin')
    tenant_name, username, password = \
        get_config('auth', 'keystone_origin_admin_credentials').split(':')

    return ksclient.Client(auth_url=orig_auth_url,
                           username=username,
                           password=password,
                           tenant_name=tenant_name)


def list_accounts(cnx):
    for x in cnx.tenants.list():
        yield x


def sync_an_account(orig_storage_url,
                    orig_token,
                    dest_storage_url,
                    dest_token):

    orig_storage_cnx = swiftclient.http_connection(orig_storage_url)
    dest_storage_cnx = swiftclient.http_connection(dest_storage_url)

    orig_account_stats, orig_containers = swiftclient.get_account(
        None, orig_token, http_conn=orig_storage_cnx, full_listing=True)

    for container in orig_containers:
        print container
        dt1 = datetime.datetime.fromtimestamp(time.time())
        sync_container(orig_storage_cnx, orig_storage_url, orig_token,
                       dest_storage_cnx, dest_storage_url, dest_token,
                       container['name'])

        dt2 = datetime.datetime.fromtimestamp(time.time())
        rd = dateutil.relativedelta.relativedelta(dt2, dt1)
        print "%d hours, %d minutes and %d seconds" % (rd.hours, rd.minutes,
                                                       rd.seconds)
        print


def sync_accounts():
    orig_auth_url = get_config('auth', 'keystone_origin')
    orig_admin_tenant, orig_admin_user, orig_admin_password = (
        get_config('auth', 'keystone_origin_admin_credentials').split(':'))
    oa_st_url, orig_admin_token = (get_auth(orig_auth_url, orig_admin_tenant,
                                            orig_admin_user,
                                            orig_admin_password))

    dest_auth_url = get_config('auth', 'keystone_dest')
    # we assume orig and dest passwd are the same obv synchronized.
    dst_st_url, dest_admin_token = (get_auth(dest_auth_url, orig_admin_tenant,
                                    orig_admin_user,
                                    orig_admin_password))

    bare_oa_st_url = oa_st_url[:oa_st_url.find('AUTH_')] + "AUTH_"
    bare_dst_st_url = dst_st_url[:dst_st_url.find('AUTH_')] + "AUTH_"

    for x in list_accounts(get_ks_auth_orig()):
        user_orig_st_url = bare_oa_st_url + x.id
        user_dst_st_url = bare_dst_st_url + x.id

        sync_an_account(user_orig_st_url,
                        orig_admin_token,
                        user_dst_st_url,
                        dest_admin_token)

if __name__ == '__main__':
    sync_accounts()
