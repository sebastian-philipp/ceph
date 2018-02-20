#!/usr/bin/env bash

killall rbd-mirror

../src/mstop.sh primary
../src/mstop.sh ceph

rm -f primary.conf
rm -f ceph.conf

MGR=1 MDS=0 ../src/mstart.sh primary -n
RGW=1 ../src/mstart.sh ceph -n

cp run/primary/ceph.conf primary.conf
cp run/ceph/ceph.conf ceph.conf

# pool creation on primary
./bin/ceph --cluster primary osd pool create rbd 100 100
./bin/ceph --cluster primary mgr module disable dashboard
./bin/ceph --cluster primary osd pool application enable rbd rbd

# starting rbd-mirror daemon in ceph site
./bin/rbd-mirror --log-file=run/ceph/out/rbd-mirror.log

# -------

# pool creation on ceph
./bin/ceph osd pool create rbd 100 100
./bin/ceph osd pool application enable rbd rbd


# enable mirroring pool mode in primary
./bin/rbd --cluster primary mirror pool enable rbd pool

# enable mirroring pool mode in ceph
./bin/rbd mirror pool enable rbd pool

# add primary cluster to ceph list of peers
./bin/rbd mirror pool peer add rbd client.admin@primary

# Now the setup is ready, each rbd image that is created in the primary cluster
# is automatically replicated to the ceph cluster

# creating img1 and run some write operations
./bin/rbd --cluster primary create --size=1G img1 --image-feature=journaling,exclusive-lock
./bin/rbd --cluster primary bench --io-total=32M --io-type=write --io-pattern=rand img1

# creating img2 and run some write operations
#./bin/rbd --cluster primary create --size=1G img2 --image-feature=journaling,exclusive-lock
#./bin/rbd --cluster primary bench --io-total=32M --io-type=write --io-pattern=rand img2

# check dashboard_v1 to see the rbd-mirroring information in the ceph cluster
