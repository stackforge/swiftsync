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
# python swift-filler.py --create -a 10 -u 1 -f 5 -c 2 -s 5000 -l
# The above command will create 10 accounts with one user in each (in keystone)
# then 2 containers will be created with 5 files in each. Each file will
# be generated with a size between 1024 Bytes to 5000 Bytes.
#
# python swift-filler.py --delete
# Read pickled index file (index_path) to process a deletion
# of objects/containers store in swift for each account then delete
# accounts.

import copy
import logging
import os
import pickle
import random
import string
import StringIO
import sys

import eventlet
from keystoneclient.exceptions import ClientException as KSClientException
from swiftclient import client as sclient
from swiftclient.client import ClientException

from swsync.utils import get_config

sys.path.append("../")
eventlet.patcher.monkey_patch()

# Some unicode codepoint
ucodes = (u'\u00b5', u'\u00c6', u'\u0159', u'\u0267',
          u'\u02b6', u'\u0370', u'\u038F', u'\u03EA',
          u'\u046A')


def get_rand_str(mode='user'):
    prefix = "%s" % mode
    return prefix + ''.join(random.choice(
        string.ascii_uppercase + string.digits) for x in range(8))


def customize(bstr, mdl):
    if mdl == 0:
        return bstr
    elif mdl == 1:
        return bstr + " s"
    elif mdl == 2:
        return (unicode(bstr, 'utf8') + u'_' +
            u"".join([random.choice(ucodes) for i in range(3)]))
    else:
        return bstr


def create_swift_user(client, account_name, account_id, user_amount):
    users = []

    def _create_user(account_name, account_id):
        user = get_rand_str(mode='user_')
        # Create a user in that tenant
        uid = client.users.create(user,
                                  get_config('filler',
                                             'default_user_password'),
                                  get_config('filler', 'default_user_email'),
                                  account_id)
        # Get swift_operator_role id
        roleid = [role.id for role in client.roles.list()
                  if role.name == get_config('filler', 'swift_operator_role')]
        if not roleid:
            logging.error('Could not find swift_operator_role %s in keystone' %
                          get_config('filler', 'swift_operator_role'))
            sys.exit(1)
        roleid = roleid[0]
        # Add tenant/user in swift operator role/group
        client.roles.add_user_role(uid.id, roleid, account_id)
        return (user, uid.id, roleid)
    for i in range(user_amount):
        try:
            ret = _create_user(account_name, account_id)
            logging.info('Users created %s in account %s' %
                         (str(ret), account_id))
            users.append(ret)
        except KSClientException:
            logging.warn('Unable to create an user in account %s' % account_id)
    return users


def create_swift_account(client, pile,
                         account_amount, user_amount,
                         index=None):

    def _create_account(user_amount):
        account = get_rand_str(mode='account_')
        # Create a tenant. In swift this is an account
        try:
            account_id = client.tenants.create(account).id
            logging.info('Account created %s' % account)
        except KSClientException:
            logging.warn('Unable to create account %s' % account)
            return None, None, None
        r = create_swift_user(client, account, account_id, user_amount)
        return account, account_id, r
    created = {}
    # Spawn a greenlet for each account
    i = 0
    for i in range(account_amount):
        i += 1
        logging.info("[Keystone Start OPs %s/%s]" % (i, account_amount))
        pile.spawn(_create_account, user_amount)
    for account, account_id, ret in pile:
        if account is not None:
            index[(account, account_id)] = ret
            created[(account, account_id)] = ret
    return created


def delete_account_content(acc, user):
    cnx = swift_cnx(acc, user[0])
    account_infos = cnx.get_account(full_listing=True)
    # Retrieve container list
    container_l = account_infos[1]
    containers_name = [ci['name'] for ci in container_l]
    # Retrieve object list
    for container in containers_name:
        container_infos = cnx.get_container(container)
        object_names = [obj_detail['name'] for obj_detail
                        in container_infos[1]]
        # Delete objects
        for obj in object_names:
            logging.info("Deleting object %s in container %s for account %s" %
                        (obj, container, str(acc)))
            cnx.delete_object(container, obj)


def delete_account(client, user_id, acc):
    account_id = acc[1]
    if not isinstance(user_id, list):
        user_id = (user_id,)
    for uid in user_id:
        logging.info("Delete user with id : %s" % uid)
        client.users.delete(uid)
    logging.info("Delete account %s" % account_id)
    client.tenants.delete(account_id)


def swift_cnx(acc, user):
    ks_url = get_config('auth', 'keystone_origin')
    cnx = sclient.Connection(ks_url,
                             user=user,
                             key=get_config('filler', 'default_user_password'),
                             tenant_name=acc[0],
                             auth_version=2)
    return cnx


def create_objects(cnx, acc, o_amount, fmax, index_containers):

    def _generate_object(f_object, size, zero_byte=False):
        if not zero_byte:
            size = random.randint(1024, size)
            end = get_rand_str('file_end_')
            f_object.seek(size - len(end))
            f_object.write(end)
            f_object.seek(0)
        else:
            f_object.seek(0)
    containers_d = index_containers[acc]
    for container, details in containers_d.items():
        for i in range(o_amount):
            f_object = StringIO.StringIO()
            if not i and o_amount > 1:
                # Generate an empty object in each container whether
                # we create more than one object
                _generate_object(f_object, fmax, zero_byte=True)
            else:
                _generate_object(f_object, fmax)
                # Customize filename
            object_name = customize(get_rand_str('file_name_'), i % 3)
            meta_keys = [customize(m, (i + 1) % 3) for m in
                         map(get_rand_str, ('X-Object-Meta-',) * 3)]
            meta_values = [customize(m, (i + 1) % 3) for m in
                           map(get_rand_str, ('meta_v_',) * 3)]
            meta = dict(zip(meta_keys, meta_values))
            data = f_object.read()
            f_object.close()
            try:
                etag = cnx.put_object(container, object_name,
                                      data, headers=copy.copy(meta))
                logging.info("Put data for container %s "
                             "(filename: %s,\tsize: %.3f KB)" %
                             (container,
                             object_name.encode('ascii', 'ignore'),
                             float(len(data)) / 1024))
                obj_info = {'object_info':
                            (object_name, etag, len(data)), 'meta': meta}
                containers_d[container]['objects'].append(obj_info)
            except ClientException:
                logging.warning('Unable to put object %s in container %s' % (
                                object_name.encode('ascii', 'ignore'),
                                container.encode('ascii', 'ignore')))


def create_containers(cnx, acc, c_amount, index_containers=None):
    containers_d = index_containers.setdefault(acc, {})
    for i in range(c_amount):
        container_name = customize(get_rand_str('container_'), i % 3)
        # python-swiftclient does not quote correctly meta ... need
        # to investigate why it does not work when key are utf8
#        meta_keys = [customize(m, (i+1)%3) for m in
#                     map(get_rand_str, ('X-Container-Meta-',) * 3)]
        meta_keys = map(get_rand_str, ('X-Container-Meta-',) * 3)
#        meta_values = map(get_rand_str, ('meta_v_',) * 3)
        meta_values = [customize(m, (i + 1) % 3) for m in
                       map(get_rand_str, ('meta_v_',) * 3)]
        meta = dict(zip(meta_keys, meta_values))
        logging.info("Create container %s" %
                     container_name.encode('ascii', 'ignore'))
        try:
            cnx.put_container(container_name, headers=copy.copy(meta))
            containers_d[container_name] = {'meta': meta, 'objects': []}
        except(ClientException), e:
            logging.warning("Unable to create container %s due to %s" %
                            (container_name.encode('ascii', 'ignore'),
                             e))


def create_account_meta(cnx):
    meta_keys = []
    meta_values = []
    for i in range(3):
        # python-swiftclient does not quote correctly meta ... need
        # to investigate why it does not work when key are utf8
        # meta_keys.extend([customize(m, (i + 1) % 3) for m in
        #             map(get_rand_str, ('X-Account-Meta-',) * 1)])
        meta_keys.extend(map(get_rand_str, ('X-Account-Meta-',) * 3))
        meta_values.extend([customize(m, (i + 1) % 3) for m in
                           map(get_rand_str, ('meta_v_',) * 1)])
    meta = dict(zip(meta_keys, meta_values))
    cnx.post_account(headers=meta)


def fill_swift(pool, created_account, c_amount,
               o_amount, fmax, index_containers=None):
    def _fill_swift_job(acc, users, c_amount,
                        o_amount, fmax, index_containers):
        cnx = swift_cnx(acc, users[0][0])
        # Use the first user we find for fill in the swift account
        # TODO(fbo) must keep track of the account meta
        create_account_meta(cnx)
        create_containers(cnx, acc, c_amount, index_containers)
        create_objects(cnx, acc, o_amount, fmax, index_containers)
    i = 0
    for acc, users in created_account.items():
        i += 1
        logging.info("[Start Swift Account OPs %s/%s]" %
                     (i, len(created_account.keys())))
        pool.spawn_n(_fill_swift_job,
                     acc, users,
                     c_amount, o_amount,
                     fmax, index_containers)
    pool.waitall()


def load_index():
    index_path = get_config('filler', 'index_path')
    if os.path.isfile(index_path):
        try:
            index = pickle.load(file(index_path))
            logging.info("Load previous index for account %s" % index_path)
        except Exception:
            index = {}
    else:
        index = {}
    return index


def load_containers_index():
    index_containers_path = get_config('filler', 'index_containers_path')
    if os.path.isfile(index_containers_path):
        try:
            index = pickle.load(file(index_containers_path))
            logging.info("Load previous index for  %s" % index_containers_path)
        except Exception:
            index = {}
    else:
        index = {}
    return index
