from cherrypy.test.helper import CPWebCase
import cherrypy
import mock
import json

from ..controllers.auth import Auth
from ..tools import SessionExpireAtBrowserCloseTool
from ..controllers.rbd_mirroring import RbdMirroring
from .helper import ControllerTestCase, authenticate

# DaemonsAndPools.get_daemons

#self._module.list_servers()

mock_list_servers = [
    {
        'services': [
            {
                'type': 'rbd-mirror',
                'id': 42
            }
        ],
        'hostname': 'localhost',
    },
]

# self._module.get_metadata('rbd-mirror', id)

mock_get_metadata = {
    'instance_id': 42,
    'id': 1,
    'ceph_version': 'mimic',
}

# self._module.get_daemon_status('rbd-mirror', id)

_status = {
    1: {
        "callouts": {},
        "image_local_count": 44,
        "image_remote_count": 45,
        "image_error_count": 46,
        "image_warning_count": 47,
        'name': 'ehh',
    }
}

mock_get_daemon_status = {
    'json': json.dumps(_status)
}

mock_osd_map = {
    'pools': [
        {
            'pool_name': 'rbd',
            'application_metadata': {'rbd'}
        }
    ]
}


#def get_pools(self, daemons):  # aus zeile 35
#    status, pool_names = self._module.rbd_pool_ls.get()


#mock_rbd_pool_ls = (
#    None,
#    ["pool1", "pool2", "pool3"]
#)

# test_rbd_mirroring.py:


#mock_rbd_pool_ls = (None, ['pool1'])


class RbdMirroringControllerTest(ControllerTestCase, CPWebCase):

    @classmethod
    def setup_server(cls):
        # Initialize custom handlers.
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler', Auth.check_auth)
        cherrypy.tools.session_expire_at_browser_close = SessionExpireAtBrowserCloseTool()

        cls._mgr_module = mock.Mock()
        cls.setup_test()

    @classmethod
    def setup_test(cls):
        mgr_mock = mock.Mock()
        mgr_mock.list_servers.return_value = mock_list_servers
        mgr_mock.get_metadata.return_value = mock_get_metadata
        mgr_mock.get_daemon_status.return_value = mock_get_daemon_status
        mgr_mock.get.return_value = mock_osd_map
        mgr_mock.url_prefix = ''

        RbdMirroring.mgr = mgr_mock
        RbdMirroring._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        cherrypy.tree.mount(RbdMirroring(), "/api/test/rbdmirroring")

    def __init__(self, *args, **kwargs):
        super(RbdMirroringControllerTest, self).__init__(*args, dashboard_port=54583, **kwargs)

    @mock.patch('dashboard_v2.controllers.rbd_mirroring.rbd')
    def test_list(self, rbd_mock):
        #rbd_mock.RBD
        self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
        self._get('/api/test/rbdmirroring')
        self.assertStatus(200)
        self.assertJsonBody({'errors': 0, 'warnings': 1})


class RbdMirrorApiTest(ControllerTestCase):

    @classmethod
    def setUpClass(cls):
        cmds = """
        # pool creation on ceph
        ceph osd pool create rbd 100 100
        ceph osd pool application enable rbd rbd
        
        # enable mirroring pool mode in primary
        rbd --cluster primary mirror pool enable rbd pool
        
        # enable mirroring pool mode in ceph
        rbd mirror pool enable rbd pool
        
        # add primary cluster to ceph list of peers
        rbd mirror pool peer add rbd client.admin@primary
        
        # Now the setup is ready, each rbd image that is created in the primary cluster
        # is automatically replicated to the ceph cluster
        
        # creating img1 and run some write operations
        rbd --cluster primary create --size=1G img1 --image-feature=journaling,exclusive-lock
        rbd --cluster primary bench --io-total=32M --io-type=write --io-pattern=rand img1
        """
        for cmd in cmds.splitlines():
            cmd = cmd.strip()
            if cmd and not cmd.startswith('#'):
                cls._cmd(cmd.split(' '))

    @classmethod
    def tearDownClass(cls):
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd', '--yes-i-really-really-mean-it'])

    @authenticate
    def test_list(self):
        self._get('/api/rbdmirroring')
        self.assertJsonBody({"errors": 0, "warnings": 1})
        self.assertStatus(200)

    @authenticate
    def test_content_data(self):
        data = self._get('/api/rbdmirroring/content_data')
        self.assertStatus(200)
        self.assertIn('daemons', data)
        self.assertEqual(len(data['daemons']), 1)
        self.assertIn('pools', data)
        self.assertEqual(len(data['pools']), 1)
        for pool_key in 'leader_id name mirror_mode health image_remote_count'.split(' '):
            self.assertIn(pool_key, data['pools'][0], pool_key)
        images_keys = 'image_error image_syncing image_ready'.split(' ')
        for image_key in images_keys:
            self.assertIn(image_key, data)

        self.assertIn('image_error', data)
        self.assertIn('image_syncing', data)
        self.assertIn('image_ready', data)
        image = sum([data[key] for key in images_keys], [])[0]
        for image_key in 'pool_name pool_name state name description'.split(' '):
            self.assertIn(image_key, image)
