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
import random
import urlparse
import uuid

STORAGE_ORIG = 'http://storage-orig.com'
STORAGE_DEST = 'http://storage-dest.com'

TENANTS_LIST = {'foo1': {'id': uuid.uuid4().hex},
                'foo2': {'id': uuid.uuid4().hex},
                'foo3': {'id': uuid.uuid4().hex}}


def gen_random_lastmodified():
    delta = datetime.timedelta(seconds=random.randint(1, 60))
    return str(datetime.datetime.now() + delta)


def gen_container(x):
    return {'name': x,
            'bytes': random.randint(1, 5000),
            'count': random.randint(1, 50),
            'bytes': random.randint(1, 5000)}


def gen_object(x):
    return {'bytes': random.randint(1, 5000),
            'last_modified': gen_random_lastmodified(),
            'name': x}

CONTAINERS_LIST = [
    (gen_container('cont1'),
     [gen_object('obj%s' % (x)) for x in xrange(random.randint(1, 10))]),
    (gen_container('cont2'),
     [gen_object('obj%s' % (x)) for x in xrange(random.randint(1, 10))]),
    (gen_container('cont3'),
     [gen_object('obj%s' % (x)) for x in xrange(random.randint(1, 10))]),
]

CONTAINER_HEADERS = {
    'x-foo': 'true', 'x-bar': 'bar',
    'x-container-object-count': '10',
    'x-container-bytes-used': '1000000',
    'x-trans-id': 'transid',
}

CONFIGDICT = {'auth':
              {'keystone_origin': STORAGE_ORIG,
               'keystone_origin_admin_credentials': 'foo1:bar:kernel',
               'keystone_dest': STORAGE_DEST}}


def fake_get_config(section, option):
    return CONFIGDICT[section][option]


class FakeSWConnection(object):
    def __init__(self, *args, **kwargs):
        self.mainargs = args
        self.mainkwargs = kwargs

    def get_auth(self, *args, **kwargs):
        tenant, user = self.mainargs[1].split(':')
        tenant_id = TENANTS_LIST[tenant]['id']
        return ('%s/v1/AUTH_%s' % (STORAGE_DEST, tenant_id), 'token')

    def get_container(*args, **kargs):
        pass

    def delete_object(self, *args, **kargs):
        pass

    def put_container(self, *args, **kargs):
        pass

    def put_object(self, *args, **kargs):
        pass

    def get_account(self, *args, **kargs):
        pass


class FakeSWObject(object):
    def __init__(self, object_name):
        pass


class FakeSWClient(object):
    @staticmethod
    def http_connection(url):
        return (urlparse.urlparse(url), None)


def fake_get_auth(auth_url, tenant, user, password):
    return FakeSWConnection(
        auth_url,
        '%s:%s' % (tenant, user),
        password,
        auth_version=2).get_auth()


class FakeKSTenant(object):
    def __init__(self, tenant_name):
        self.tenant_name = tenant_name

    @property
    def id(self):
        return TENANTS_LIST[self.tenant_name]['id']

    def __str__(self):
        return self.tenant_name


class FakeKSUser(object):
    def __init__(self):
        self.id = uuid.uuid4().hex


class FakeKSClientUsers(object):
    def create(self, *args):
        return FakeKSUser()

    def delete(self, *args):
        pass


class FakeKSRole(object):
    def __init__(self):
        self.id = uuid.uuid4().hex
        self.name = 'Member'


class FakeKSClientRoles(object):
    def add_user_role(self, *args):
        pass

    def list(self):
        return [FakeKSRole(), ]


class FakeKSClientTenant(object):
    def list(self):
        for t in list(TENANTS_LIST):
            yield FakeKSTenant(t)

    def create(self, account):
        return FakeKSTenant(TENANTS_LIST.keys()[0])

    def delete(self, *args):
        pass


class FakeKSClient(object):
    def __init__(self, *args):
        self.args = args
        self.tenants = FakeKSClientTenant()
        self.roles = FakeKSClientRoles()
        self.users = FakeKSClientUsers()

    def __call__(self):
        return self.args


class FakeKS(object):
    @staticmethod
    def Client(*args, **kwargs):
        return FakeKSClient(args, kwargs)
