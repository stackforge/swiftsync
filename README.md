A massive Swift syncer
======================

The purpose of this tool is to give you a way to migrate
the entire content of a swift cluster to an another by
using swift REST API.

The swiftsync project come with two tools:

 - swfiller
 - swsync

The first one will ease swsync testing by the way
you will be able to populate a test cluster easily
by filling it quickly with heterogeneous data.

The second is the syncer. Basically it will read
origin swift cluster account by account and
perform the data synchronization by taking care
of avoiding synchronization for data already up to
date.

Run unit tests
--------------

Unitests can be run quickly just after cloning
the project from github.

    $ sudo pip install tox
    $ cd swiftsync
    $ tox

Run functional tests
--------------------

You can easily start functional tests for swsync
tool by installing two swift on devstack and setting
the ResellerAdmin role to the admin user in keystone
(refer to swsync usage later in this readme) and start
nose as follow :

    $ nosetests -v --nologcapture tests/functional/test_syncer.py

swiftsync installation
----------------------

Prepare a python virtual environment and start
setup.py install.

    $ virtualenv $HOME/venv
    $ . $HOME/venv/bin/activate
    $ pip install -r tools/pip-requires
    $ python setup.py install

Note, without the manual pip install, the installation might failed with
this error: 'TypeError: dist must be a Distribution instance'
ref: https://bugs.launchpad.net/swift/+bug/1217288

swfiller usage
--------------

This script aims to fill in a swift cluster with random
data. A custom amount of account will be created against keystone
then many containers and objects will be pushed to those accounts.
Accounts and objects will be flavored with some random meta data.

Two indexes will be pickled to filesystem to store first
which accounts has been created and second
which containers/objects + MD5 and metadata
has been stored.

This script use eventlet to try to speedup the most
the fill in process. Default concurrency value can be modified
in configuration file.

Before using the filler you need to add a configuration file
by copying the sample one (etc/config-sample.ini) and then
editing keystone_origin address and keystone_origin_admin_credential
(tenant:username:password). Be sure to use an user with keystone
admin role to let the filler create tenants and users.

Which kind of randomization the filler will add to data:

* random account name
* random metadata on account (some will contain unicode)
* random container name
* random metadata on container (some will contain unicode)
* random object name with random garbage data in it
* random metadata on object (some will contain unicode)
* some object will be created with empty content

The command below will fill in the swift cluster:

    $ swfiller --create -a 10 -u 1 -c 10 -f 10 -s 1024 --config etc/config.ini

Meaning of the options are as follow:

* --create : creating mode (there also a deletion mode to clean tenant and data)
* -a : amount of account or tenant to create
* -u : amount of user to create or each account
* -c : amount of container to create for each account
* -f : amount of file to create in each container
* -s : the maximum size for file to create (in Bytes)

As mention above there is also a deletion mode that use
index files created during fill in operations. Index files
will keep a list a user and tenant we have created in keystone.
So to clean all account and data the create mode has created
use the deletion mode:

    $ swfiller --delete --config etc/config.ini

swsync usage
------------

The synchronization process will not handle keystone synchronization.
Database synchronization will need to be done by configuring
the replication capabilities of the keystone database.

The user used by the sync tool will need to be able to perform
API operations on each account for both origin and destination
cluster. To do that the user must own the role ResellerAdmin.

Adding the role ResellerAdmin to admin user in keystone is
straightforward by using the following command (be sure
to have properly set your environment variables before):

    $ keystone user-role-add --tenant admin --user \
      admin --role ResellerAdmin

swsync will replicate :

* account and account metadata
* container and container metadata
* object and object metadata

The way it will act to do that is as follow:

* will synchronize account metadata if they has changed on origin
* will delete container on destination if no longer exists on origin
* will create container on destination if not exists
* will synchronize destination container metadata if not same
  as origin container.
* will remove container object if no longer exists in origin container
* will synchronize object and metadata object if the last-modified header
  is the lastest on the origin.


To start the synchronization process you need to edit
the configuration file and configure keystone_dest
and keystone_dest_credentials. Then to start
the process simply :

    $ swsync etc/config.ini

As mention above the sync process won't
replicate origin keystone accounts to the destination 
keystone so swift accounts on destination will
not work until you start a keystone database synchronization. But be sure
when performing the database synchronization to have swift endpoints
configured to reference the destination swift.

swsync will take care of already synchronized containers/objects. When
re-starting swsync it will only synchronize data that have changed.
swsync has been designed to be run and run again and not ensuring that the
first pass goes well, if for example there is network failure swsync will
just skip it and hope to do it on the next run. So the tool can for instance
be launched by a cron job to perform diff synchronization each night.

Tenant Filter File
------------------

It is possible to limit the migration to a subset of the total number of
tenants, by uncommenting the field "tenant_filter_file". This field should
hold the path to a file containing a list of tenant names to migrate, one
per line. If left commented, swsync will migrate all the tenants.

Swift Middleware last-modified
------------------------------

A swift middleware has been written to speedup the
synchronization process by adding a last modified metadata
to container header. The idea behind this is to only
process the container whether the timestamp is greater
on origin avoiding uselessly walking through container.
When performing some tests we figured out that synchronization
performances was fast enough for our use case so we decided
to not support this metadata in swsync for now. But If you want to
contribute feel free to add it !


Things to considers
-------------------

swfiller and swsync are not designed to work with swift v1.0 authentication.
We experienced some performances troubles when doing large synchronization
with token validation. Having to validate the token each time could come back with
error due to keystone capability to handle large amount of token validation requests.


Reporting a bug
---------------

The issue tracker is managed by launchpad so please use the
following link to report a bug :

https://bugs.launchpad.net/swiftsync

If you want to submit a patch please use https://review.openstack.org.
If you are not familiar with the Openstack way of submitting patches
please read before http://docs.openstack.org/infra/manual/developers.html.
