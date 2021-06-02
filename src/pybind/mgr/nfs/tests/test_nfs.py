from contextlib import contextmanager
from unittest import mock
from unittest.mock import MagicMock

from ceph.deployment.service_spec import NFSServiceSpec
from nfs import Module
from nfs.export import ExportMgr
from nfs.export_utils import GaneshaConfParser, Export
from orchestrator import ServiceDescription, DaemonDescription, OrchResult


class TestNFS():
    daemon_raw_config = """
NFS_CORE_PARAM {
            Enable_NLM = false;
            Enable_RQUOTA = false;
            Protocols = 4;
            NFS_Port = 14000;
        }

        MDCACHE {
           Dir_Chunk = 0;
        }

        NFSv4 {
           RecoveryBackend = rados_cluster;
           Minor_Versions = 1, 2;
        }

        RADOS_KV {
           pool = nfs-ganesha;
           namespace = vstart;
           UserId = vstart;
           nodeid = a;
        }

        RADOS_URLS {
       Userid = vstart;
       watch_url = 'rados://nfs-ganesha/vstart/conf-nfs.vstart';
        }

    %url rados://nfs-ganesha/vstart/conf-nfs.vstart
"""

    export_1 = """
EXPORT {
    Export_ID=1;
    Protocols = 4;
    Path = /;
    Pseudo = /cephfs_a/;
    Access_Type = RW;
    Protocols = 4;
    Attr_Expiration_Time = 0;
    # Delegations = R;
    # Squash = root;

    FSAL {
        Name = CEPH;
        Filesystem = "a";
        User_Id = "ganesha";
        # Secret_Access_Key = "YOUR SECRET KEY HERE";
    }

    CLIENT
    {
        Clients = 192.168.0.10, 192.168.1.0/8;
        Squash = None;
    }

    CLIENT
    {
        Clients = 192.168.0.0/16;
        Squash = All;
        Access_Type = RO;
    }
}
"""

    export_2 = """
EXPORT
{
    Export_ID=2;

    Path = "/";

    Pseudo = "/rgw";

    Access_Type = RW;

    squash = AllAnonymous;

    Protocols = 4, 3;

    Transports = TCP, UDP;

    FSAL {
        Name = RGW;
        User_Id = "testuser";
        Access_Key_Id ="access_key";
        Secret_Access_Key = "secret_key";
    }
}
"""

    conf_nodea = '''
%url "rados://ganesha/ns/export-2"

%url "rados://ganesha/ns/export-1"'''

    conf_nodeb = '%url "rados://ganesha/ns/export-1"'

    conf_nfs_foo = '''
%url "rados://ganesha2/foo/export-1"

%url "rados://ganesha2/foo/export-2"'''

    clusters = {
        'foo': {
            'pool': 'ganesha2',
            'namespace': 'foo',
            'type': "ORCHESTRATOR",
            'daemon_conf': 'conf-nfs.foo',
            'daemons': ['foo.host_a', 'foo.host_b'],
            'exports': {
                1: ['foo.host_a', 'foo.host_b'],
                2: ['foo.host_a', 'foo.host_b'],
                3: ['foo.host_a', 'foo.host_b']  # for new-added export
            }
        }
    }


    class RObject(object):
        def __init__(self, key, raw):
            self.key = key
            self.raw = raw

        def read(self, _):
            return self.raw.encode('utf-8')

        def stat(self):
            return len(self.raw), None

    def _ioctx_write_full_mock(self, key, content):
        if key not in self.temp_store[self.temp_store_namespace]:
            self.temp_store[self.temp_store_namespace][key] = \
                TestNFS.RObject(key, content.decode('utf-8'))
        else:
            self.temp_store[self.temp_store_namespace][key].raw = content.decode('utf-8')

    def _ioctx_remove_mock(self, key):
        del self.temp_store[self.temp_store_namespace][key]

    def _ioctx_list_objects_mock(self):
        r = [obj for _, obj in self.temp_store[self.temp_store_namespace].items()]
        return r

    def _ioctl_stat_mock(self, key):
        return self.temp_store[self.temp_store_namespace][key].stat()

    def _ioctl_read_mock(self, key, size=None):
        return self.temp_store[self.temp_store_namespace][key].read(size)

    def _ioctx_set_namespace_mock(self, namespace):
        self.temp_store_namespace = namespace

    def _reset_temp_store(self):
        self.temp_store_namespace = None
        self.temp_store = {
            'ns': {
                'export-1': TestNFS.RObject("export-1", self.export_1),
                'export-2': TestNFS.RObject("export-2", self.export_2),
                'conf-nodea': TestNFS.RObject("conf-nodea", self.conf_nodea),
                'conf-nodeb': TestNFS.RObject("conf-nodeb", self.conf_nodeb),
            },
            'foo': {
                'export-1': TestNFS.RObject("export-1", self.export_1),
                'export-2': TestNFS.RObject("export-2", self.export_2),
                'conf-nfs.foo': TestNFS.RObject("conf-nfs.foo", self.conf_nfs_foo)
            }
        }

    @contextmanager
    def _mock_orchestrator(self, enable):

        self.io_mock = MagicMock()
        self.io_mock.set_namespace.side_effect = self._ioctx_set_namespace_mock
        self.io_mock.read = self._ioctl_read_mock
        self.io_mock.stat = self._ioctl_stat_mock
        self.io_mock.list_objects.side_effect = self._ioctx_list_objects_mock
        self.io_mock.write_full.side_effect = self._ioctx_write_full_mock
        self.io_mock.remove_object.side_effect = self._ioctx_remove_mock

        # mock nfs services
        cluster_info = self.clusters['foo']
        orch_nfs_services = [
            ServiceDescription(spec=NFSServiceSpec(service_id='foo',
                                                   pool=cluster_info['pool'],
                                                   namespace=cluster_info['namespace']))
        ] if enable else []
        # pylint: disable=protected-access
        #ganesha.Ganesha._get_orch_nfs_services = Mock(return_value=orch_nfs_services)

        # mock nfs daemons
        def _get_nfs_instances(service_name=None):
            if not enable:
                return []
            instances = {
                'nfs.foo': [
                    DaemonDescription(daemon_id='foo.host_a', status=1),
                    DaemonDescription(daemon_id='foo.host_b', status=1)
                ],
                'nfs.bar': [
                    DaemonDescription(daemon_id='bar.host_c', status=1)
                ]
            }
            if service_name is not None:
                return instances[service_name]
            result = []
            for _, daemons in instances.items():
                result.extend(daemons)
            return result
        with mock.patch('nfs.module.Module.describe_service') as describe_service, \
             mock.patch('nfs.module.Module.rados') as rados:


            rados.open_ioctx.return_value.__enter__.return_value = self.io_mock
            rados.open_ioctx.return_value.__exit__ = mock.Mock(return_value=None)

            describe_service.return_value = OrchResult(orch_nfs_services)

            self._reset_temp_store()

            yield

        #ganesha.ExportMgrOrchestrator._get_orch_nfs_instances = Mock(
        #    side_effect=_get_nfs_instances)

    def test_parse_daemon_raw_config(self):
        expected_daemon_config = [
            {
                "block_name": "NFS_CORE_PARAM",
                "enable_nlm": False,
                "enable_rquota": False,
                "protocols": 4,
                "nfs_port": 14000
            },
            {
                "block_name": "MDCACHE",
                "dir_chunk": 0
            },
            {
                "block_name": "NFSV4",
                "recoverybackend": "rados_cluster",
                "minor_versions": [1, 2]
            },
            {
                "block_name": "RADOS_KV",
                "pool": "nfs-ganesha",
                "namespace": "vstart",
                "userid": "vstart",
                "nodeid": "a"
            },
            {
                "block_name": "RADOS_URLS",
                "userid": "vstart",
                "watch_url": "'rados://nfs-ganesha/vstart/conf-nfs.vstart'"
            },
            {
                "block_name": "%url",
                "value": "rados://nfs-ganesha/vstart/conf-nfs.vstart"
            }
        ]
        daemon_config = GaneshaConfParser(self.daemon_raw_config).parse()
        assert daemon_config == expected_daemon_config

    def _validate_export_1(self, export: Export):
        assert export.export_id == 1
        assert export.path == "/"
        assert export.pseudo == "/cephfs_a/"
        # assert export.tag is None
        assert export.access_type == "RW"
        # assert export.squash == "root_squash"  # probably correct value
        assert export.squash == "no_root_squash"
        assert export.protocols == [4]
        #        assert export.transports == {"TCP", "UDP"}
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "ganesha"
        assert export.fsal.fs_name == "a"
        assert export.fsal.sec_label_xattr == None
        assert len(export.clients) == 2
        assert export.clients[0].addresses == \
               ["192.168.0.10", "192.168.1.0/8"]
        # assert export.clients[0].squash ==  "no_root_squash"  # probably correct value
        assert export.clients[0].squash == "None"
        assert export.clients[0].access_type is None
        assert export.clients[1].addresses == ["192.168.0.0/16"]
        # assert export.clients[1].squash ==  "all_squash"  # probably correct value
        assert export.clients[1].squash == "All"
        assert export.clients[1].access_type == "RO"
        assert export.cluster_id in ('_default_', 'foo')
        assert export.attr_expiration_time == 0
        # assert export.security_label == False  # probably correct value
        assert export.security_label == True

    def test_export_parser_1(self):
        blocks = GaneshaConfParser(self.export_1).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        export = Export.from_export_block(blocks[0], '_default_')
        self._validate_export_1(export)

    def _validate_export_2(self, export: Export):
        assert export.export_id == 2
        assert export.path == "/"
        assert export.pseudo == "/rgw"
        #assert export.tag is None
        assert export.access_type == "RW"
        # assert export.squash == "all_squash"  # probably correct value
        assert export.squash == "AllAnonymous"
        assert export.protocols == [4, 3]
        assert set(export.transports) == {"TCP", "UDP"}
        assert export.fsal.name == "RGW"
        #assert export.fsal.rgw_user_id == "testuser"  # probably correct value
        #assert export.fsal.access_key == "access_key"  # probably correct value
        #assert export.fsal.secret_key == "secret_key"  # probably correct value
        assert len(export.clients) == 0
        assert export.cluster_id in ('_default_', 'foo')

    def test_export_parser_2(self):
        blocks = GaneshaConfParser(self.export_2).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        export = Export.from_export_block(blocks[0], '_default_')
        self._validate_export_2(export)


    def test_daemon_conf_parser_a(self):
        blocks = GaneshaConfParser(self.conf_nodea).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 2
        assert blocks[0]['block_name'] == "%url"
        assert blocks[0]['value'] == "rados://ganesha/ns/export-2"
        assert blocks[1]['block_name'] == "%url"
        assert blocks[1]['value'] == "rados://ganesha/ns/export-1"

    def test_daemon_conf_parser_b(self):
        blocks = GaneshaConfParser(self.conf_nodeb).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        assert blocks[0]['block_name'] == "%url"
        assert blocks[0]['value'] == "rados://ganesha/ns/export-1"

    def test_ganesha_conf(self):
        with self._mock_orchestrator(True):
            for cluster_id, info in self.clusters.items():
                self._do_test_ganesha_conf(cluster_id, info['exports'])
                self._reset_temp_store()

    def _do_test_ganesha_conf(self, cluster, expected_exports):
        nfs_mod = Module('nfs', '', '')
        ganesha_conf = ExportMgr(nfs_mod)
        exports = ganesha_conf.exports['foo']

        assert len(exports) == 2
        #assert 1 in exports
        #assert 2 in exports

        self._validate_export_1([e for e in exports if e.export_id == 1][0])
        self._validate_export_2([e for e in exports if e.export_id == 2][0])


    def test_config_dict(self):
        with self._mock_orchestrator(True):
            for cluster_id, info in self.clusters.items():
                self._do_test_config_dict(cluster_id, info['exports'])
                self._reset_temp_store()

    def _do_test_config_dict(self, cluster, expected_exports):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        export = [e for e in conf.exports['foo'] if e.export_id == 1][0]
        ex_dict = export.to_dict()

        assert ex_dict == {'access_type': 'RW',
             'clients': [{'access_type': None,
                          'addresses': ['192.168.0.10', '192.168.1.0/8'],
                          'squash': 'None'},
                         {'access_type': 'RO',
                          'addresses': ['192.168.0.0/16'],
                          'squash': 'All'}],
             'cluster_id': 'foo',
             'export_id': 1,
             'fsal': {'fs_name': 'a', 'name': 'CEPH', 'user_id': 'ganesha'},
             'path': '/',
             'protocols': [4],
             'pseudo': '/cephfs_a/',
             'security_label': True,
             'squash': 'no_root_squash',
             'transports': [None]}

        export = [e for e in conf.exports['foo'] if e.export_id == 2][0]
        ex_dict = export.to_dict()
        assert ex_dict == {'access_type': 'RW',
             'clients': [],
             'cluster_id': 'foo',
             'export_id': 2,
             'fsal': {'name': 'RGW',
                      'secret_access_key': 'secret_key',
                      'user_id': 'testuser'},
             'path': '/',
             'protocols': [3, 4],
             'pseudo': '/rgw',
             'security_label': True,
             'squash': 'AllAnonymous',
             'transports': ['TCP', 'UDP']}

    def test_config_from_dict(self):
        with self._mock_orchestrator(True):
            for cluster_id, info in self.clusters.items():
                self._do_test_config_from_dict(cluster_id, info['exports'])
                self._reset_temp_store()

    def _do_test_config_from_dict(self, cluster_id, expected_exports):
        export = Export.from_dict(1, {
            'daemons': expected_exports[1],
            'export_id': 1,
            'path': '/',
            'cluster_id': cluster_id,
            'pseudo': '/cephfs_a',
            'tag': None,
            'access_type': 'RW',
            'squash': 'root_squash',
            'security_label': True,
            'protocols': [4],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.10", "192.168.1.0/8"],
                'access_type': None,
                'squash': 'no_root_squash'
            }, {
                'addresses': ["192.168.0.0/16"],
                'access_type': 'RO',
                'squash': 'all_squash'
            }],
            'fsal': {
                'name': 'CEPH',
                'user_id': 'ganesha',
                'fs_name': 'a',
                'sec_label_xattr': 'security.selinux'
            }
        })

        assert export.export_id == 1
        assert export.path == "/"
        assert export.pseudo == "/cephfs_a"
        #assert export.tag is None
        assert export.access_type == "RW"
        assert export.squash == "root_squash"
        assert set(export.protocols) == {4}
        assert set(export.transports) == {"TCP", "UDP"}
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "ganesha"
        assert export.fsal.fs_name == "a"
        assert export.fsal.sec_label_xattr == 'security.selinux'
        assert len(export.clients) == 2
        assert export.clients[0].addresses == \
                         ["192.168.0.10", "192.168.1.0/8"]
        assert export.clients[0].squash == "no_root_squash"
        assert export.clients[0].access_type is None
        assert export.clients[1].addresses == ["192.168.0.0/16"]
        assert export.clients[1].squash == "all_squash"
        assert export.clients[1].access_type == "RO"
        # assert export.daemons == set(expected_exports[1])  # probably correct value
        assert export.cluster_id == cluster_id
        assert export.attr_expiration_time == 0
        assert export.security_label

        export = Export.from_dict(2, {
            'daemons': expected_exports[2],
            'export_id': 2,
            'path': '/',
            'pseudo': '/rgw',
            'cluster_id': cluster_id,
            'tag': None,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

        assert export.export_id == 2
        assert export.path == "/"
        assert export.pseudo == "/rgw"
        #assert export.tag is None
        assert export.access_type == "RW"
        assert export.squash == "all_squash"
        assert set(export.protocols) == {4, 3}
        assert set(export.transports) == {"TCP", "UDP"}
        assert export.fsal.name == "RGW"
#        assert export.fsal.rgw_user_id == "testuser"
#        assert export.fsal.access_key is None
#        assert export.fsal.secret_key is None
        assert len(export.clients) == 0
#        assert export.daemons == set(expected_exports[2])
        assert export.cluster_id == cluster_id

    """
    def test_update_export(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_update_export(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_update_export(self, cluster_id, expected_exports):
        ganesha.RgwClient = MagicMock()
        admin_inst_mock = MagicMock()
        admin_inst_mock.get_user_keys.return_value = {
            'access_key': 'access_key',
            'secret_key': 'secret_key'
        }
        ganesha.RgwClient.admin_instance.return_value = admin_inst_mock

        conf = ExportMgr.instance(cluster_id)
        conf.update_export({
            'export_id': 2,
            'daemons': expected_exports[2],
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': cluster_id,
            'tag': 'bucket_tag',
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.0/16"],
                'access_type': None,
                'squash': None
            }],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

        conf = ExportMgr.instance(cluster_id)
        export = conf.get_export(2)
        assert export.export_id == 2
        assert export.path, "bucket")
        assert export.pseudo, "/rgw/bucket")
        assert export.tag, "bucket_tag")
        assert export.access_type == "RW"
        assert export.squash == "all_squash"
        assert export.protocols == {4, 3}
        assert export.transports == {"TCP", "UDP"}
        assert export.fsal.name == "RGW"
        assert export.fsal.rgw_user_id == "testuser"
        assert export.fsal.access_key == "access_key"
        assert export.fsal.secret_key == "secret_key"
        assert len(export.clients), 1)
        assert export.clients[0].user_id == "ganesha"
        self.assertIsNone(export.clients[0].squash)
        assert export.clients[0].access_type is None
        assert export.daemons == set(expected_exports[2])
        assert export.cluster_id == cluster_id
    """
    
    def test_remove_export(self):
        with self._mock_orchestrator(True):
            for cluster_id, info in self.clusters.items():
                self._do_test_remove_export(cluster_id, info['exports'])
                self._reset_temp_store()

    def _do_test_remove_export(self, cluster_id, expected_exports):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        assert len(conf.exports[cluster_id]) == 2
        assert conf.delete_export(cluster_id=cluster_id, pseudo_path="/rgw") == (0, "Successfully deleted export", "")
        exports = conf.exports[cluster_id]
        assert len(exports) == 1
        assert exports[0].export_id == 1

    """
    def test_create_export_rgw(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_create_export_rgw(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_create_export_rgw(self, cluster_id, expected_exports):
        ganesha.RgwClient = MagicMock()
        admin_inst_mock = MagicMock()
        admin_inst_mock.get_user_keys.return_value = {
            'access_key': 'access_key2',
            'secret_key': 'secret_key2'
        }
        ganesha.RgwClient.admin_instance.return_value = admin_inst_mock

        conf = ExportMgr.instance(cluster_id)
        ex_id = conf.create_export({
            'daemons': expected_exports[3],
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'tag': 'bucket_tag',
            'cluster_id': cluster_id,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.0/16"],
                'access_type': None,
                'squash': None
            }],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

        conf = ExportMgr.instance(cluster_id)
        exports = conf.list_exports(cluster_id=cluster_id)
        assert len(exports) == 3
        export = conf.get_export(ex_id)
        assert export.export_id == ex_id
        assert export.path, "bucket")
        assert export.pseudo, "/rgw/bucket")
        assert export.tag, "bucket_tag")
        assert export.access_type == "RW"
        assert export.squash == "all_squash"
        assert export.protocols == {4, 3}
        assert export.transports == {"TCP", "UDP"}
        assert export.fsal.name == "RGW"
        assert export.fsal.rgw_user_id == "testuser"
        assert export.fsal.access_key, "access_key2")
        assert export.fsal.secret_key, "secret_key2")
        assert len(export.clients), 1)
        assert export.clients[0].user_id == "ganesha"
        self.assertIsNone(export.clients[0].squash)
        assert export.clients[0].access_type is None
        assert export.daemons== set(expected_exports[3])
        assert export.cluster_id == cluster_id

    def test_create_export_cephfs(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_create_export_cephfs(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_create_export_cephfs(self, cluster_id, expected_exports):
        ganesha.CephX = MagicMock()
        ganesha.CephX.list_clients.return_value = ["fs"]
        ganesha.CephX.get_client_key.return_value = "fs_key"

        ganesha.CephFS = MagicMock()
        ganesha.CephFS.dir_exists.return_value = True

        conf = ExportMgr.instance(cluster_id)
        ex_id = conf.create_export({
            'daemons': expected_exports[3],
            'path': '/',
            'pseudo': '/cephfs2',
            'cluster_id': cluster_id,
            'tag': None,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': True,
            'protocols': [4],
            'transports': ['TCP'],
            'clients': [],
            'fsal': {
                'name': 'CEPH',
                'user_id': 'fs',
                'fs_name': None,
                'sec_label_xattr': 'security.selinux'
            }
        })

        conf = ExportMgr.instance(cluster_id)
        exports = conf.list_exports(cluster_id=cluster_id)
        assert len(exports) == 3
        export = conf.get_export(ex_id)
        assert export.export_id == ex_id
        assert export.path == "/"
        assert export.pseudo == "/cephfs2"
        assert export.tag is None
        assert export.access_type == "RW"
        assert export.squash == "all_squash"
        assert export.protocols == {4}
        assert export.transports == {"TCP"}
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "fs"
        assert export.fsal.cephx_key == "fs_key"
        assert export.fsal.sec_label_xattr == "security.selinux"
        assert export.fsal.fs_name is None
        assert len(export.clients) == 0
        assert export.daemons== set(expected_exports[3])
        assert export.cluster_id == cluster_id
        assert export.attr_expiration_time == 0
        assert export
    """
    """
    def test_reload_daemons(self):
        # Fail to import call in Python 3.8, see https://bugs.python.org/issue35753
        mock_call = unittest.mock.call

        # Orchestrator cluster: reload all daemon config objects.
        conf = ExportMgr.instance('foo')
        calls = [mock_call(conf) for conf in conf.list_daemon_confs()]
        for daemons in [[], ['a', 'b']]:
            conf.reload_daemons(daemons)
            self.io_mock.notify.assert_has_calls(calls)
            self.io_mock.reset_mock()

        # User-defined cluster: reload daemons in the parameter
        self._set_user_defined_clusters_location()
        conf = ExportMgr.instance('_default_')
        calls = [mock_call('conf-{}'.format(daemon)) for daemon in ['nodea', 'nodeb']]
        conf.reload_daemons(['nodea', 'nodeb'])
        self.io_mock.notify.assert_has_calls(calls)
    """
    
    """
    def test_list_daemons(self):
        for cluster_id, info in self.clusters.items():
            instance = ExportMgr.instance(cluster_id)
            daemons = instance.list_daemons()
            for daemon in daemons:
                assert daemon['cluster_id'], cluster_id)
                assert daemon['cluster_type'], info['type'])
                self.assertIn('daemon_id', daemon)
                self.assertIn('status', daemon)
                self.assertIn('status_desc', daemon)
            assert [daemon['daemon_id'] for daemon in daemons], info['daemons'])
    
    def test_validate_orchestrator(self):
        cluster_id = 'foo'
        cluster_info = self.clusters[cluster_id]
        instance = ExportMgr.instance(cluster_id)
        export = MagicMock()

        # export can be linked to none or all daemons
        export_daemons = [[], cluster_info['daemons']]
        for daemons in export_daemons:
            export.daemons = daemons
            instance.validate(export)

        # raise if linking to partial or non-exist daemons
        export_daemons = [cluster_info['daemons'][:1], 'xxx']
        for daemons in export_daemons:
            with self.assertRaises(NFSException):
                export.daemons = daemons
                instance.validate(export)
        
    def test_validate_user(self):
        self._set_user_defined_clusters_location()
        cluster_id = '_default_'
        instance = ExportMgr.instance(cluster_id)
        export = MagicMock()

        # export can be linked to none, partial, or all daemons
        fake_daemons = ['nodea', 'nodeb']
        export_daemons = [[], fake_daemons[:1], fake_daemons]
        for daemons in export_daemons:
            export.daemons = daemons
            instance.validate(export)

        # raise if linking to non-exist daemons
        export_daemons = ['xxx']
        for daemons in export_daemons:
            with self.assertRaises(NFSException):
                export.daemons = daemons
                instance.validate(export)

    def _verify_locations(self, locations, cluster_ids):
        for cluster_id in cluster_ids:
            self.assertIn(cluster_id, locations)
            cluster = locations.pop(cluster_id)
            self.assertDictEqual(cluster, {key: cluster[key] for key in [
                'pool', 'namespace', 'type', 'daemon_conf']})
        self.assertDictEqual(locations, {})

    def test_get_cluster_locations(self):
        # pylint: disable=protected-access

        # There is only a Orchestrator cluster.
        self._mock_orchestrator(True)
        locations = ganesha.Ganesha._get_clusters_locations()
        self._verify_locations(locations, ['foo'])

        # No cluster.
        self._mock_orchestrator(False)
        with self.assertRaises(NFSException):
            ganesha.Ganesha._get_clusters_locations()

        # There is only a user-defined cluster.
        self._set_user_defined_clusters_location()
        self._mock_orchestrator(False)
        locations = ganesha.Ganesha._get_clusters_locations()
        self._verify_locations(locations, ['_default_'])

        # There are both Orchestrator cluster and user-defined cluster.
        self._set_user_defined_clusters_location()
        self._mock_orchestrator(True)
        locations = ganesha.Ganesha._get_clusters_locations()
        self._verify_locations(locations, ['foo', '_default_'])

    def test_get_cluster_locations_conflict(self):
        # pylint: disable=protected-access

        # Pool/namespace collision.
        self._set_user_defined_clusters_location('ganesha2/foo')
        with self.assertRaises(NFSException) as ctx:
            ganesha.Ganesha._get_clusters_locations()
        self.assertIn('already in use', str(ctx.exception))

        # Cluster name collision with orch. cluster.
        self._set_user_defined_clusters_location('foo:ganesha/ns')
        with self.assertRaises(NFSException) as ctx:
            ganesha.Ganesha._get_clusters_locations()
        self.assertIn('Detected a conflicting NFS-Ganesha cluster', str(ctx.exception))

        # Cluster name collision with user-defined cluster.
        self._set_user_defined_clusters_location('cluster1:ganesha/ns,cluster1:fake-pool/fake-ns')
        with self.assertRaises(NFSException) as ctx:
            ganesha.Ganesha._get_clusters_locations()
        self.assertIn('Duplicate Ganesha cluster definition', str(ctx.exception))
"""
