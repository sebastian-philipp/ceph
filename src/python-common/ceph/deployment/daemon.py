"""Bootstrap a local Ceph cluster using systemd and containers

Usage:
  ceph-daemon bootstrap [--mon-name=name] [--fsid=fsid] [options]
  ceph-daemon create_initial_keyring [options]
  ceph-daemon deploy mon [--mon-name=name] [options]
  ceph-daemon deploy mgr <name> [options]
  ceph-daemon version [options]
  ceph-daemon --version

Options:
  -h --help            Show this screen.
  --version            Show local Ceph version.
  --image=<image>      Imaged used for deploying the daemon  [default: ceph/daemon-base:latest-master]
  --fsid=fsid          FSID of the new cluster. Optional
  --mon-name=name      Name of the new MON. Default is `hostname`
  --cluster-addr=addr  Address:Port of the new mon.
  --public-addr=addr   IP of new mon.
  --uid=uid            user id for folders and daemons
  --gid=gid            group id for folders and daemons
"""
import logging

import docopt
from ceph.deployment import ssh_orchestrator

def main():
    logging.basicConfig(level=logging.DEBUG)
    args = docopt.docopt(__doc__)
    image = args['--image']
    if args['version']:
        ssh_orchestrator.get_ceph_version(image)
    elif args['bootstrap']:
        ssh_orchestrator.bootstrap_cluster(image=image,
                                           fsid=args['--fsid'],
                                           cluster_addr=args['--cluster-addr'],
                                           public_addr=args['--public-addr'])
