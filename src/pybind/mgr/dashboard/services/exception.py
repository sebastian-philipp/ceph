# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
from contextlib import contextmanager
from functools import wraps

import cherrypy

import rbd
import rados

from .. import logger
from ..services.ceph_service import RadosReturnError


class ViewCacheNoDataException(Exception):
    def __init__(self):
        self.status = 200
        super(ViewCacheNoDataException, self).__init__('ViewCache: unable to retrieve data')

class DashboardException(Exception):
    """
    Used for exceptions that are already handled and should end up as a user error.

    Typically, you don't inherent from DashboardException
    Or, as a replacement for cherrypy.HTTPError(...)
    """

    # pylint: disable=too-many-arguments
    def __init__(self, e=None, code=None, component=None, http_status_code=None, msg=None):
        super(DashboardException, self).__init__(msg)
        self._code = code
        self.component = component
        if e:
            self.e = e
        if http_status_code:
            self.status = http_status_code

    def __str__(self):
        try:
            return str(self.e)
        except AttributeError:
            return super(DashboardException, self).__str__()

    @property
    def errno(self):
        return self.e.errno

    @property
    def code(self):
        if self._code:
            return str(self._code)
        return str(abs(self.errno))


def serialize_dashboard_exception(e):
    cherrypy.response.status = getattr(e, 'status', 400)
    cherrypy.response.headers['Content-Type'] = 'application/json'
    out = dict(detail=str(e))
    try:
        out['errno'] = e.errno
    except AttributeError:
        pass
    try:
        out['code'] = e.code
    except AttributeError:
        pass
    component = getattr(e, 'component', None)
    out['component'] = component if component else None
    return out


def dashboard_exception_handler(handler, *args, **kwargs):
    from ..tools import ViewCache

    try:
        with handle_rados_error(component=None):  # make the None controller the fallback.
            return handler(*args, **kwargs)
    # Don't catch cherrypy.* Exceptions.
    except ViewCacheNoDataException as e:
        logger.exception('dashboard_exception_handler')
        cherrypy.response.headers['Content-Type'] = 'application/json'
        cherrypy.response.status = getattr(e, 'status', 400)
        return json.dumps({'status': ViewCache.VALUE_NONE, 'value': None}).encode('utf-8')
    except DashboardException as e:
        logger.exception('dashboard_exception_handler')
        return json.dumps(serialize_dashboard_exception(e)).encode('utf-8')


@contextmanager
def handle_rbd_error():
    try:
        yield
    except rbd.OSError as e:
        raise DashboardException(e, component='rbd')
    except rbd.Error as e:
        raise DashboardException(e, component='rbd', code=e.__class__.__name__)

@contextmanager
def handle_rados_error(component):
    try:
        yield
    except rados.OSError as e:
        raise DashboardException(e, component=component)
    except rados.Error as e:
        raise DashboardException(e, component=component, code=e.__class__.__name__)

@contextmanager
def handle_send_command_error(component):
    try:
        yield
    except RadosReturnError as e:
        raise DashboardException(e, component=component)


def c2d(my_contextmanager, *cargs, **ckwargs):
    """Converts a contextmanager into a decorator. Only needed for Python 2"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            with my_contextmanager(*cargs, **ckwargs):
                return f(*args, **kwargs)

        return wrapper
    return decorator
