# -*- encoding: utf-8 -*-
__author__ = "Chmouel Boudjnah <chmouel@chmouel.com>"
import uuid

TENANTS_LIST = {'foo1': {'id': uuid.uuid4().hex},
                'foo2': {'id': uuid.uuid4().hex},
                'foo3': {'id': uuid.uuid4().hex}}

CONFIGDICT = {'auth':
              {'keystone_origin': 'http://keystone-origin.com',
               'keystone_origin_admin_credentials': 'foo1:bar:kernel',
               'keystone_dest': 'http://storage-dest.com'}}

STORAGE_DEST = 'http://storage-dest.com'


def fake_get_config(section, option):
    return CONFIGDICT[section][option]


def fake_get_auth(auth_url, tenant, user, password):
    return FakeSWConnection(
        auth_url,
        '%s:%s' % (tenant, user),
        password,
        auth_version=2).get_auth()


class FakeSWConnection(object):
    def __init__(self, *args, **kwargs):
        self.mainargs = args
        self.mainkwargs = kwargs

    def get_auth(self, *args, **kwargs):
        tenant, user = self.mainargs[1].split(':')
        tenant_id = TENANTS_LIST[tenant]['id']
        return ('%s/v1/AUTH_%s' % (STORAGE_DEST, tenant_id), 'token')


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
