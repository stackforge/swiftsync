# -*- python -*-
# Author(s): Fabien Boucher <fabien.boucher@enovance.com>
#
# This script aims to fill in a swift cluster with random
# data.
# A custom amount of account will be created against keystone
# then many containers and objects will be pushed to those accounts.
# Accounts and objects will be flavored with some random meta data.
#
# Two indexes will be pickled to FS to store first which accounts has been
# created (index_path) and second which containers/objects + MD5 and meta data
# has been stored (index_containers_path).
#
# This script use eventlet to try to speedup the most
# the fill in process.
#
# Usage:
#
# python swift-filer.py --create -a 10 -u 1 -f 5 -c 2 -s 5000 -l
# The above command will create 10 accounts with one user in each (in keystone)
# then 2 containers will be created with 5 files in each. Each file will
# be generated with a size between 1024 Bytes to 5000 Bytes.
#
# python swift-filer.py --delete
# Read pickled index file (index_path) to process a deletion
# of objects/containers store in swift for each account then delete
# accounts.


import os
import sys
import argparse
import pickle
import random
import string

from StringIO import StringIO
from copy import copy

from keystoneclient.v2_0 import client
from swiftclient import client as sclient

import eventlet

eventlet.patcher.monkey_patch()

# Need to adapt for your configuration #
a_username = 'admin'
a_password = 'wxcvbn'
auth_url = 'http://127.0.0.1:5000/v2.0'
a_tenant = 'admin'
swift_operator_role = 'Member'
##

default_user_password = 'password'
default_user_email = 'johndoe@domain.com'
index_path = '/tmp/swift_filer_index.pkl'
index_containers_path = '/tmp/swift_filer_containers_index.pkl'


def get_rand_str(mode='user'):
    prefix = "%s" % mode
    return prefix + ''.join(random.choice( \
                string.ascii_uppercase + string.digits) for x in range(8))


def create_swift_user(account_name, account_id, user_amount):
    users = []

    def _create_user(account_name, account_id):
        user = get_rand_str(mode='user_')
        # Create a user in that tenant
        uid = c.users.create(user, default_user_password,
                             default_user_email, account_id)
        # Get swift_operator_role id
        roleid = [role.id for role in c.roles.list()
                    if role.name == swift_operator_role][0]
        # Add tenant/user in swift operator role/group
        c.roles.add_user_role(uid.id, roleid, account_id)
        return (user, uid.id, roleid)
    for i in range(user_amount):
        ret = _create_user(account_name, account_id)
        users.append(ret)
    return users


def create_swift_account(account_amount, user_amount, index=None):

    def _create_account(user_amount):
        account = get_rand_str(mode='account_')
        # Create a tenant. In swift this is an account
        account_id = c.tenants.create(account).id
        print 'Account created %s' % account
        r = create_swift_user(account, account_id, user_amount)
        print 'Users created %s in account %s' % (str(r), account)
        return account, account_id, r
    created = {}
    # Spawn a greenlet for each account
    for i in range(account_amount):
        pile.spawn(_create_account, user_amount)
    for account, account_id, ret in pile:
        index[(account, account_id)] = ret
        created[(account, account_id)] = ret
    return created


def delete_account_content(acc, user):
    cnx = swift_cnx(acc, user[0])
    account_infos = cnx.get_account(full_listing=True)
    # Retreive container list
    container_l = account_infos[1]
    containers_name = [ci['name'] for ci in container_l]
    # Retreive object list
    for container in containers_name:
        container_infos = cnx.get_container(container)
        object_names = [obj_detail['name'] for obj_detail in container_infos[1]]
        # Delete objects
        for obj in object_names:
            print "Deleting object %s in container %s for account %s" \
                    % (obj, container, str(acc))
            cnx.delete_object(container, obj)


def delete_account(user_id, acc):
    account_id = acc[1]
    if not isinstance(user_id, list):
        user_id = (user_id)
    for uid in user_id:
        print "Delete user with id : %s" % uid
        c.users.delete(uid)
    print "Delete account %s" % account_id
    c.tenants.delete(account_id)


def swift_cnx(acc, user):
    cnx = sclient.Connection(auth_url,
                            user=user,
                            key=default_user_password,
                            tenant_name=acc[0],
                            auth_version=2)
    return cnx


def create_objects(cnx, acc, o_amount, fmax, index_containers):

    def _generate_object(f_object, size):
        size = random.randint(1024, size)
        end = get_rand_str('file_end_')
        f_object.seek(size - len(end))
        f_object.write(end)
        f_object.seek(0)
    containers_d = index_containers[acc]
    for container, details in containers_d.items():
        for i in range(o_amount):
            print "Put data for container %s" % container
            f_object = StringIO()
            _generate_object(f_object, fmax)
            object_name = get_rand_str('file_name_')
            meta_keys = map(get_rand_str, ('X-Object-Meta-',) * 3)
            meta_values = map(get_rand_str, ('meta_v_',) * 3)
            meta = dict(zip(meta_keys, meta_values))
            etag = cnx.put_object(container, object_name,
                              f_object.read(), headers=copy(meta))
            f_object.close()
            obj_info = {'object_info': (object_name, etag), 'meta': meta}
            containers_d[container]['objects'].append(obj_info)


def create_containers(cnx, acc, c_amount, index_containers=None):
    containers_d = index_containers.setdefault(acc, {})
    for i in range(c_amount):
        container_name = get_rand_str('container_')
        meta_keys = map(get_rand_str, ('X-Container-Meta-',) * 3)
        meta_values = map(get_rand_str, ('meta_v_',) * 3)
        meta = dict(zip(meta_keys, meta_values))
        print "Create container %s" % container_name
        cnx.put_container(container_name, headers=copy(meta))
        containers_d[container_name] = {'meta': meta, 'objects': []}


def fill_swift(created_account, c_amount, o_amount, fmax, index_containers=None):
    def _fill_swift_job(acc, users, c_amount, o_amount, fmax, index_containers):
        cnx = swift_cnx(acc, users[0][0])
        # Use the first user we find for fill in the swift account
        create_containers(cnx, acc, c_amount, index_containers)
        create_objects(cnx, acc, o_amount, fmax, index_containers)
    for acc, users in created_account.items():
        pool.spawn_n(_fill_swift_job,
                     acc, users,
                     c_amount, o_amount,
                     fmax, index_containers)
    pool.waitall()


def load_index():
    if os.path.isfile(index_path):
        try:
            index = pickle.load(file(index_path))
            print "Load previous index for account %s" % index_path
        except:
            index = {}
    else:
        index = {}
    return index


def load_containers_index():
    if os.path.isfile(index_containers_path):
        try:
            index = pickle.load(file(index_containers_path))
            print "Load previous index for  %s" % index_containers_path
        except:
            index = {}
    else:
        index = {}
    return index


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='swift-filler', add_help=True)
    parser.add_argument('--delete',
                        action='store_true',
                        help='Suppress created accounts/users')
    parser.add_argument('--create',
                        action='store_true',
                        help='Create account/users/containers/data')
    parser.add_argument('-l',
                        action='store_true',
                        help='Load previous indexes and append newly created to it')
    parser.add_argument('-a',
                        help='Specify account amount')
    parser.add_argument('-u',
                        help='Specify user amount by account')
    parser.add_argument('-c',
                        help='Specify container amount by account')
    parser.add_argument('-f',
                        help='Specify file amount by account')
    parser.add_argument('-s',
                        help='Specify the MAX file size. Files will be from 1024 Bytes to MAX Bytes')
    args = parser.parse_args()

    pile = eventlet.GreenPile(20)
    pool = eventlet.GreenPool(20)

    c = client.Client(username=a_username,
                      password=a_password,
                      auth_url=auth_url,
                      tenant_name=a_tenant)

    if not args.create and not args.delete:
        parser.print_help()
        sys.exit(1)
    if args.create and args.delete:
        parser.print_help()
        sys.exit(1)

    if args.l:
        index = load_index()
        index_containers = load_containers_index()
    else:
        index = {}
        index_containers = {}
    if args.create:
        if args.a is None or not args.a.isdigit():
            print("Provide account amount by setting '-a' option")
            sys.exit(1)
        if args.u is None or not args.u.isdigit():
            print("Provide user by account amount by setting '-u' option")
            sys.exit(1)
        if args.s is None:
            fmax = 1024
        else:
            if args.s.isdigit():
                fmax = int(args.s)
            else:
                fmax = 1024
        created = create_swift_account(int(args.a), int(args.u), index=index)
        if args.f is not None and args.c is not None:
            if args.f.isdigit() and args.c.isdigit():
                fill_swift(created, int(args.c),
                           int(args.f), fmax, index_containers=index_containers)
            else:
                print("'-c' and '-f' options must be integers")
                sys.exit(1)
        pickle.dump(index, file(index_path, 'w'))
        pickle.dump(index_containers, file(index_containers_path, 'w'))
    if args.delete:
        index = load_index()
        for k, v in index.items():
            user_info_list = [user[1] for user in v]
            # Take the first user we find
            delete_account_content(k, v[0])
            delete_account(user_info_list, k)
            del index[k]
        pickle.dump(index, file(index_path, 'w'))
