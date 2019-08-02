"""
Bootstrap MON TODOs:

* [X]: generate a unique uuid,
* [ ]: pick the mon IP address (from whatever is on the host),
* [X]: ``--fsid <cluster-uuid>``
* [X]: ``--name <mon-name>`` - default to hostname
* [ ]: ``--mon-host <mon host list>`` - existing mon ips to probe
* [ ]: ``--addr <ip-or-iplist>`` or ``--addrv <...>`` - IP(s) for the mon to bind to
* [ ]: ``--key <secret>`` - the ``mon.`` key, so that a new mon can join the existing mons
* [ ]:...and so on.

1. [X] something that runs the daemon with misc args in the container, for one-off things like mkfs, ceph-osd --mkfs, or even ceph-bluestore-tool,
2. [X] something that creates the systemd unit to run the daemon, and
3. [X] one that drops the unit.

* [ ]: lightly modified scheme, like ``/var/lib/ceph/$uuid/mon/$id``, that behaves a bit better when there are multiple clusters
"""

import os
import logging
import sys
from distutils.spawn import find_executable
from os.path import expanduser
from subprocess import check_output, CalledProcessError
from typing import List, Dict, Sequence

from attr import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CephContainer(object):
    image: str
    entrypoint: str
    args: List[str]
    volume_mounts: Dict[str, str]
    name: str = None
    podman_args: List[str] = []

    @property
    def run_cmd(self):
        vols = sum([['-v', f'{host_dir}:{container_dir}']
                    for host_dir, container_dir in self.volume_mounts.items()], [])
        envs = ['-e', f'CONTAINER_IMAGE={self.image}', '-e', f'NODE_NAME={get_hostname()}', ]
        name = ['--name', self.name] if self.name else []
        return [
            find_program('podman'),
            'run',
            '--rm',
            '--net=host',
            ] + self.podman_args + name + envs + vols + [
            '--entrypoint', f'/usr/bin/{self.entrypoint}',
            self.image
        ] + self.args

    def run(self):
        logger.info(self.run_cmd)
        check_output(self.run_cmd)


def get_ceph_version(image):
    CephContainer(image, 'ceph', ['--version']).run()

def ceph_cli(image, args):
    try:
        CephContainer(image,
                      entrypoint='ceph',
                      args=args,
                      volume_mounts={
                          '/var/lib/ceph': '/var/lib/ceph:z',
                          '/var/run/ceph': '/var/run/ceph:z',
                          '/etc/localtime': '/etc/localtime:ro',
                          '/var/log/ceph': '/var/log/ceph:z'
                      },
                      ).run()
    except CalledProcessError as e:
        logger.info(f'{e}')
        sys.exit(1)

def bootstrap_cluster(image, fsid=None, mon_name=None, cluster_addr=None, public_addr=None, uid=0, gid=0):
    fsid = fsid or make_fsid()
    mon_name = mon_name or get_hostname()
    assert cluster_addr, 'TODO: make proper default'
    assert public_addr, 'TODO: make proper default'

    mon_keyring_path = create_initial_keyring(image)
    create_mon(image, mon_keyring_path, fsid, mon_name=mon_name, uid=uid, gid=gid)
    start_mon(image, fsid, mon_name, mon_keyring_path, cluster_addr, public_addr,
              mon_initial_members=mon_name, uid=uid, gid=gid)

    create_mgr()

def create_initial_keyring(image):
    mon_keyring_path = '/var/lib/ceph/tmp'
    mon_keyring = f'{mon_keyring_path}/keyring'

    makedirs(mon_keyring_path)

    CephContainer(
        image=image,
        entrypoint='ceph-authtool',
        args=f'--create-keyring {mon_keyring} --gen-key -n mon.'.split(),
        volume_mounts={'/var/lib/ceph/': '/var/lib/ceph'}
    ).run()

    logger.info(f'{mon_keyring} created')
    return mon_keyring

def create_mon(image, mon_keyring_path, fsid, mon_name, uid=0, gid=0):
    mon_path = f"/var/lib/ceph/mon/ceph-{mon_name}"
    makedirs(mon_path)

    CephContainer(
        image=image,
        entrypoint='ceph-mon',
        args=['--mkfs',
              '-i', mon_name,
              '--fsid', fsid,
              '--keyring', mon_keyring_path
              ] + user_args(uid, gid),
        volume_mounts={'/var/lib/ceph/': '/var/lib/ceph'}
    ).run()

def start_mon(image, fsid, mon_name, mon_keyring_path, cluster_addr, public_addr,
              mon_initial_members=None, uid=0, gid=0):
    makedirs('/var/run/ceph')
    mon_container = CephContainer(
        image=image,
        entrypoint='ceph-mon',
        args=['-i', mon_name,
              '--fsid', fsid,
              '--keyring', mon_keyring_path,
              f'--cluster_addr={cluster_addr}',
              f'--public_addr={public_addr}',
              f'--mon_initial_members={mon_name}',
              '-f', # foreground
              '-d' # log to stderr
              ] + user_args(uid, gid),
        volume_mounts={
            '/var/lib/ceph': '/var/lib/ceph:z',
            '/var/run/ceph': '/var/run/ceph:z',
            '/etc/localtime': '/etc/localtime:ro',
            '/var/log/ceph': '/var/log/ceph:z'
        },
        name='ceph-mon-%i',
    )
    unit_path = expanduser('~/.config/systemd/user')  # TODO: use system-wide location
    makedirs(unit_path)
    logger.info(mon_container.run_cmd)
    with open(f'{unit_path}/ceph-mon@.service', 'w') as f:
        f.write(f"""[Unit]
Description=Ceph Monitor
After=network.target

[Service]
EnvironmentFile=-/etc/environment
ExecStartPre=-/usr/bin/podman rm ceph-mon-%i
ExecStart={' '.join(mon_container.run_cmd)}
ExecStop=-/usr/bin/podman stop ceph-mon-%i
ExecStopPost=-/bin/rm -f /var/run/ceph/ceph-mon.%i.asok
Restart=always
RestartSec=10s
TimeoutStartSec=120
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
""")
    check_output(['systemctl', '--user', 'disable', f'ceph-mon@{mon_name}.service'])
    check_output(['systemctl', '--user', 'enable', f'ceph-mon@{mon_name}.service'])
    check_output(['systemctl', '--user', 'start', f'ceph-mon@{mon_name}.service'])
    logger.info(f'See > journalctl --user -f -u ceph-mon@{mon_name}.service')

def create_mgr():
    pass


# Utils

def user_args(uid, gid):
    user_args = []
    if uid != 0:
        user_args = user_args + ['--setuser', str(uid)]
    if gid != 0:
        user_args = user_args + ['--setgroup', str(gid)]
    return user_args


def get_hostname():
    import socket
    return socket.gethostname()

def make_fsid():
    import uuid
    return str(uuid.uuid1())

def find_program(filename):
    name = find_executable(filename)
    if name is None:
        raise ValueError(f'{filename} not found')
    return name

def makedirs(dir):
    os.makedirs(dir, exist_ok=True)

