# -*- coding: utf-8 -*-
"""
openATTIC mgr plugin (based on CherryPy)
"""
from __future__ import absolute_import

import errno
import os
import socket
import tempfile
from uuid import uuid4

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

from OpenSSL import crypto
import cherrypy
from mgr_module import MgrModule, MgrStandbyModule

if 'COVERAGE_ENABLED' in os.environ:
    import coverage
    _cov = coverage.Coverage(config_file="{}/.coveragerc".format(os.path.dirname(__file__)))
    _cov.start()

# pylint: disable=wrong-import-position
from . import logger, mgr
from .controllers.auth import Auth
from .tools import load_controllers, json_error_page, SessionExpireAtBrowserCloseTool, \
                   NotificationQueue


# cherrypy likes to sys.exit on error.  don't let it take us down too!
# pylint: disable=W0613
def os_exit_noop(*args):
    pass


# pylint: disable=W0212
os._exit = os_exit_noop


def configure_cherrypy(default_port, module):
    def prepare_url_prefix(url_prefix):
        """
        :return: '' if no prefix, or '/prefix' without slash in the end.
        """
        url_prefix = urljoin('/', url_prefix)
        return url_prefix.rstrip('/')

    server_addr = module.get_localized_config('server_addr', '::')
    server_port = module.get_localized_config('server_port', str(default_port))
    if server_addr is None:
        raise RuntimeError(
            'no server_addr configured; '
            'try "ceph config-key put mgr/{}/{}/server_addr <ip>"'
                .format(module.module_name, module.get_mgr_id()))
    module.log.info('server_addr: %s server_port: %s', server_addr,
                    server_port)

    url_prefix = prepare_url_prefix(module.get_config('url_prefix',
                                                             default=''))

    # SSL initialization

    cert = module.get_config("crt")
    if cert is not None:
        module.cert_tmp = tempfile.NamedTemporaryFile()
        module.cert_tmp.write(cert.encode('utf-8'))
        module.cert_tmp.flush()  # cert_tmp must not be gc'ed
        cert_fname = module.cert_tmp.name
    else:
        cert_fname = module.get_localized_config('crt_file')

    pkey = module.get_config("key")
    if pkey is not None:
        module.pkey_tmp = tempfile.NamedTemporaryFile()
        module.pkey_tmp.write(pkey.encode('utf-8'))
        module.pkey_tmp.flush()  # pkey_tmp must not be gc'ed
        pkey_fname = module.pkey_tmp.name
    else:
        pkey_fname = module.get_localized_config('key_file')

    if not cert_fname or not pkey_fname:
        logger.warning('{}, {}, {}, {}'.format(cert, pkey, cert_fname, pkey_fname))
        raise RuntimeError('no certificate configured')
    if not os.path.isfile(cert_fname):
        raise RuntimeError('certificate %s does not exist' % cert_fname)
    if not os.path.isfile(pkey_fname):
        raise RuntimeError('private key %s does not exist' % pkey_fname)

    # Apply the 'global' CherryPy configuration.
    config = {
        'engine.autoreload.on': False,
        'server.socket_host': server_addr,
        'server.socket_port': int(server_port),
        'error_page.default': json_error_page,

        'server.ssl_module': 'builtin',
        #'server.ssl_module': 'pyopenssl',
        'server.ssl_certificate': cert_fname,
        'server.ssl_private_key': pkey_fname
    }
    cherrypy.config.update(config)

    return server_addr, server_port, url_prefix


class Module(MgrModule):
    """
    dashboard module entrypoint
    """

    COMMANDS = [
        {
            'cmd': 'dashboard set-login-credentials '
                   'name=username,type=CephString '
                   'name=password,type=CephString',
            'desc': 'Set the login credentials',
            'perm': 'w'
        },
        {
            'cmd': 'dashboard set-session-expire '
                   'name=seconds,type=CephInt',
            'desc': 'Set the session expire timeout',
            'perm': 'w'
        },
        {
            "cmd": "dashboard create-self-signed-cert",
            "desc": "Create self signed certificate",
            "perm": "w"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        mgr.init(self)
        self.url_prefix = ''

    def _serve(self):
        server_addr, server_port, self.url_prefix = configure_cherrypy(8080, self)

        logger.warning('server_addr, server_port, self.url_prefix: {}, {}, {}'.format(server_addr, server_port, self.url_prefix))

        # Initialize custom handlers.
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler', Auth.check_auth)
        cherrypy.tools.session_expire_at_browser_close = SessionExpireAtBrowserCloseTool()

        current_dir = os.path.dirname(os.path.abspath(__file__))
        fe_dir = os.path.join(current_dir, 'frontend/dist')
        config = {
            '/': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': fe_dir,
                'tools.staticdir.index': 'index.html'
            }
        }

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri("http://{0}:{1}{2}/".format(
            socket.getfqdn() if server_addr == "::" else server_addr,
            server_port,
            self.url_prefix
        ))

        cherrypy.tree.mount(Module.ApiRoot(self), '{}/api'.format(self.url_prefix))
        cherrypy.tree.mount(Module.StaticRoot(), '{}/'.format(self.url_prefix), config=config)

    def serve(self):
        if 'COVERAGE_ENABLED' in os.environ:
            _cov.start()
        NotificationQueue.start_queue()

        while True:
            try:
                self._serve()

                cherrypy.engine.start()
                logger.info('Waiting for engine...')
                cherrypy.engine.block()
                break
            except RuntimeError:
                self.log.exception('Cannot server')
                import time
                time.sleep(5)


        if 'COVERAGE_ENABLED' in os.environ:
            _cov.stop()
            _cov.save()
        logger.info('Engine done')

    def shutdown(self):
        super(Module, self).shutdown()
        logger.info('Stopping server...')
        NotificationQueue.stop()
        cherrypy.engine.exit()
        logger.info('Stopped server')

    def handle_command(self, cmd):
        if cmd['prefix'] == 'dashboard set-login-credentials':
            Auth.mgr = self
            Auth.set_login_credentials(cmd['username'], cmd['password'])
            return 0, 'Username and password updated', ''
        elif cmd['prefix'] == 'dashboard set-session-expire':
            self.set_config('session-expire', str(cmd['seconds']))
            return 0, 'Session expiration timeout updated', ''
        elif cmd['prefix'] == 'dashboard create-self-signed-cert':
            self.create_self_signed_cert()
            return 0, 'Self-signed Certificate created', ''

        return (-errno.EINVAL, '', 'Command not found \'{0}\''
                .format(cmd['prefix']))

    def create_self_signed_cert(self):
        try:
            # create a key pair
            pkey = crypto.PKey()
            pkey.generate_key(crypto.TYPE_RSA, 2048)

            # create a self-signed cert
            cert = crypto.X509()
            cert.get_subject().O = "IT"
            cert.get_subject().CN = "ceph-dashboard"
            cert.set_serial_number(int(uuid4()))
            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(10*365*24*60*60)
            cert.set_issuer(cert.get_subject())
            cert.set_pubkey(pkey)
            cert.sign(pkey, 'sha512')

            cert = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
            self.set_config('crt', cert.decode('utf-8'))

            pkey = crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey)
            self.set_config('key', pkey.decode('utf-8'))
        except Exception:
            logger.exception('huh?')

    def notify(self, notify_type, notify_id):
        NotificationQueue.new_notification(notify_type, notify_id)

    class ApiRoot(object):

        _cp_config = {
            'tools.sessions.on': True,
            'tools.authenticate.on': True
        }

        def __init__(self, mgrmod):
            self.ctrls = load_controllers()
            logger.debug('Loaded controllers: %s', self.ctrls)

            first_level_ctrls = [ctrl for ctrl in self.ctrls
                                 if '/' not in ctrl._cp_path_]
            multi_level_ctrls = set(self.ctrls).difference(first_level_ctrls)

            for ctrl in first_level_ctrls:
                logger.info('Adding controller: %s -> /api/%s', ctrl.__name__,
                            ctrl._cp_path_)
                inst = ctrl()
                setattr(Module.ApiRoot, ctrl._cp_path_, inst)

            for ctrl in multi_level_ctrls:
                path_parts = ctrl._cp_path_.split('/')
                path = '/'.join(path_parts[:-1])
                key = path_parts[-1]
                parent_ctrl_classes = [c for c in self.ctrls
                                       if c._cp_path_ == path]
                if len(parent_ctrl_classes) != 1:
                    logger.error('No parent controller found for %s! '
                                 'Please check your path in the ApiController '
                                 'decorator!', ctrl)
                else:
                    inst = ctrl()
                    setattr(parent_ctrl_classes[0], key, inst)

        @cherrypy.expose
        def index(self):
            tpl = """API Endpoints:<br>
            <ul>
            {lis}
            </ul>
            """
            endpoints = ['<li><a href="{}">{}</a></li>'.format(ctrl._cp_path_, ctrl.__name__) for
                         ctrl in self.ctrls]
            return tpl.format(lis='\n'.join(endpoints))

    class StaticRoot(object):
        pass


class StandbyModule(MgrStandbyModule):
    def __init__(self, *args, **kwargs):
        super(StandbyModule, self).__init__(*args, **kwargs)
        self.url_prefix = ''

    def serve(self):
        _, _, url_prefix = configure_cherrypy(7000, self)

        module = self

        class Root(object):
            @cherrypy.expose
            def index(self):
                active_uri = module.get_active_uri()
                if active_uri:
                    module.log.info("Redirecting to active '%s'", active_uri)
                    raise cherrypy.HTTPRedirect(active_uri)
                else:
                    template = """
                <html>
                    <!-- Note: this is only displayed when the standby
                         does not know an active URI to redirect to, otherwise
                         a simple redirect is returned instead -->
                    <head>
                        <title>Ceph</title>
                        <meta http-equiv="refresh" content="{delay}">
                    </head>
                    <body>
                        No active ceph-mgr instance is currently running
                        the dashboard.  A failover may be in progress.
                        Retrying in {delay} seconds...
                    </body>
                </html>
                    """
                    return template.format(delay=5)

        cherrypy.tree.mount(Root(), "{}/".format(url_prefix), {})
        self.log.info("Starting engine...")
        cherrypy.engine.start()
        self.log.info("Waiting for engine...")
        cherrypy.engine.wait(state=cherrypy.engine.states.STOPPED)
        self.log.info("Engine done.")

    def shutdown(self):
        self.log.info("Stopping server...")
        cherrypy.engine.wait(state=cherrypy.engine.states.STARTED)
        cherrypy.engine.stop()
        self.log.info("Stopped server")
