swift-filler.py
==============

Author(s): Fabien Boucher <fabien.boucher@enovance.com>

Description
-----------

This script aims to fill in a swift cluster with random
data.
A custom amount of account will be created against keystone
then many containers and objects will be pushed to those accounts.
Accounts and objects will be flavored with some random meta data.

Two indexes will be pickled to FS to store first which accounts has been
created (index_path) and second which containers/objects + MD5 and meta data
has been stored (index_containers_path).

This script use eventlet to try to speedup the most
the fill in process.


Usage
-----

To fill the cluster ::

 python swift-filler.py --create -a 10 -u 1 -f 5 -c 2 -s 5000 -l

The above command will create 10 accounts with one user in each (in keystone)
then 2 containers will be created with 5 files in each. Each file will
be generated with a size between 1024 Bytes to 5000 Bytes.

To delete data ::

 python swift-filler.py --delete

Read pickled index file (index_path) to process a deletion
of objects/containers store in swift for each account then delete
accounts.
