from dashboard_v2.tests.helper import ControllerTestCase, authenticate


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
