import json
import mock

import cherrypy
from cherrypy.test.helper import CPWebCase

from ..controllers.auth import Auth
from ..tools import SessionExpireAtBrowserCloseTool
from ..controllers.rbd_mirroring import RbdMirror
from .helper import ControllerTestCase


mock_list_servers = [{
    'hostname': 'ceph-host',
    'services': [{'id': 3, 'type': 'rbd-mirror'}]
}]

mock_get_metadata = {
    'id': 1,
    'instance_id': 3,
    'ceph_version': 'ceph version 13.0.0-5719 mimic (dev)'
}

_status = {
    1: {
        'callouts': {},
        'image_local_count': 5,
        'image_remote_count': 6,
        'image_error_count': 7,
        'image_warning_count': 8,
        'name': 'pool_name'
    }
}

mock_get_daemon_status = {
    'json': json.dumps(_status)
}

mock_osd_map = {
    'pools': [{
        'pool_name': 'rbd',
        'application_metadata': {'rbd'}
    }]
}


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
        cls.mock_rbd = mock.patch('dashboard_v2.controllers.rbd_mirroring.rbd')
        cls.mock_rbd.start()

        mgr_mock = mock.Mock()
        mgr_mock.list_servers.return_value = mock_list_servers
        mgr_mock.get_metadata.return_value = mock_get_metadata
        mgr_mock.get_daemon_status.return_value = mock_get_daemon_status
        mgr_mock.get.return_value = mock_osd_map
        mgr_mock.url_prefix = ''

        RbdMirror.mgr = mgr_mock
        RbdMirror._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        cherrypy.tree.mount(RbdMirror(), '/api/test/rbdmirror')
        cherrypy.tree.mount(RbdMirror().toplevel(), '/api/test/rbdmirror/toplevel')

    @classmethod
    def tearDownClass(cls):
        cls.mock_rbd.stop()

    def __init__(self, *args, **kwargs):
        super(RbdMirroringControllerTest, self).__init__(*args, dashboard_port=54583, **kwargs)

    def test_default(self):
        self._get('/api/test/rbdmirror')
        self.assertStatus(200)
        self.assertJsonBody({'status': 0, 'content_data': {
            'daemons': [{
                'id': 1,
                'instance_id': 3,
                'version': 'ceph version 13.0.0-5719 mimic (dev)',
                'server_hostname': 'ceph-host',
                'service': {'id': 3, 'type': 'rbd-mirror'},
                'server': {'hostname': 'ceph-host',
                           'services': [{'id': 3, 'type': 'rbd-mirror'}]},
                'metadata': {'id': 1,
                             'instance_id': 3,
                             'ceph_version': 'ceph version 13.0.0-5719 mimic (dev)'},
                'status': {'1': {'callouts': {},
                                 'image_local_count': 5,
                                 'image_remote_count': 6,
                                 'image_error_count': 7,
                                 'image_warning_count': 8,
                                 'name': 'pool_name'}},
                'health_color': 'success', 'health': 'OK'}],
            'pools': [{
                'name': 'rbd',
                'health_color': 'warning',
                'health': 'Warning',
                'mirror_mode': 'unknown'}],
            'image_error': [],
            'image_syncing': [],
            'image_ready': []},
        })

    def test_toplevel(self):
        self._get('/api/test/rbdmirror/toplevel')
        self.assertStatus(200)
        self.assertJsonBody({'errors': 0, 'warnings': 0})
