# -*- encoding: utf-8 -*-
__author__ = "Chmouel Boudjnah <chmouel@chmouel.com>"
import uuid
import random

STORAGE_ORIG = 'http://storage-orig.com'
STORAGE_DEST = 'http://storage-dest.com'

TENANTS_LIST = {'foo1': {'id': uuid.uuid4().hex},
                'foo2': {'id': uuid.uuid4().hex},
                'foo3': {'id': uuid.uuid4().hex}}

CONTAINERS_LIST = [{'count': random.randint(1, 100),
                   'name': "cont1",
                   'bytes': random.randint(1, 10000)},
                   {'count': random.randint(1, 100),
                    'name': "cont2",
                    'bytes': random.randint(1, 10000)},
                   {'count': random.randint(1, 100),
                    'name': "cont3",
                    'bytes': random.randint(1, 10000)}]

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


class FakeSWContainer(object):
    def __init__(self, dico):
        self.dico = dico

    def __getitem__(self, key):
        if key == 'name':
            return self.dico['name']

    def __repr__(self):
        return str(self.dico)


class FakeSWClient(object):
    @staticmethod
    def http_connection(url):
        #TODO:
        return "cnx"

    @staticmethod
    def get_account(*args, **kwargs):
        return (random.randint(1, 9999),
                [FakeSWContainer(x) for x in CONTAINERS_LIST])


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


class FakeKSClientTenant(object):
    def list(self):
        for t in list(TENANTS_LIST):
            yield FakeKSTenant(t)


class FakeKSClient(object):
    def __init__(self, *args):
        self.args = args
        self.tenants = FakeKSClientTenant()

    def __call__(self):
        return self.args


class FakeKS(object):
    @staticmethod
    def Client(*args, **kwargs):
        return FakeKSClient(args, kwargs)
