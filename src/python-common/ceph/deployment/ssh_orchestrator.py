import os

def get_ceph_version(image):
    os.system(f"podman run --rm --net=host --entrypoint /usr/bin/ceph {image} --version")


def bootstrap_cluster():
    create_mon()
    create_mgr()

def create_mon():
    pass

def create_mgr():
    pass
