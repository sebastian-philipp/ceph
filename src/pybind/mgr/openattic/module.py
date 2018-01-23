# -*- coding: utf-8 -*-

"""
openATTIC mgr plugin (based on CherryPy)
"""
import os
import cherrypy
from cherrypy import tools

from mgr_module import MgrModule

# cherrypy likes to sys.exit on error.  don't let it take us down too!
def os_exit_noop():
    pass

os._exit = os_exit_noop

"""
openATTIC CherryPy Module
"""
class Module(MgrModule):

    """
    Hello.
    """

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

    def serve(self):
        cherrypy.config.update({'server.socket_host': '0.0.0.0',
                                'server.socket_port': 8080,
                               })
        cherrypy.tree.mount(Module.HelloWorld(self), "/")
        cherrypy.engine.start()
        self.log.info("Waiting for engine...")
        cherrypy.engine.block();
        self.log.info("Engine done.")

    def shutdown(self):
        self.log.info("Stopping server...")
        cherrypy.engine.exit()
        self.log.info("Stopped server")

    def handle_command(self, cmd):
        pass

    class HelloWorld(object):

        """

        Hello World.

        """

        def __init__(self, module):
            self.module = module
            self.log = module.log
            self.log.warn("Initiating WebServer CherryPy")

        @cherrypy.expose
        def index(self):
            """
            WS entrypoint
            """

            return "Hello World!"

        @cherrypy.expose
        @tools.json_out()
        def ping(self):
            """
            Ping endpoint
            """

            return "pong"
