"""Bootstrap a local Ceph cluster using systemd and containers

Usage:
  ceph-daemon bootstrap [options]
  ceph-daemon deploy mon <name> [options]
  ceph-daemon deploy mgr <name> [options]
  ceph-daemon version [options]
  ceph-daemon --version

Options:
  -h --help           Show this screen.
  --version           Show local Ceph version.
  --image=<image>     Imaged used for deploying the daemon  [default: ceph/daemon-base:latest-master]
"""
import docopt
from ceph.deployment import ssh_orchestrator

def main():
    args = docopt.docopt(__doc__)
    if args['version']:
        ssh_orchestrator.get_ceph_version(args['--image'])


