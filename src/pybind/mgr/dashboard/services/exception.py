# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
from contextlib import contextmanager
from functools import partial

import cherrypy
from cherrypy._cptools import Tool

import rbd
import rados

from .. import logger
from ..services.ceph_service import SendCommandError


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


def browsable_exception(func):
    def wrapper(e):
        from ..tools import browsable_api_enabled, render_browsable_api
        if not browsable_api_enabled():
            return json.dumps(func(e)).encode('utf-8')

        return render_browsable_api(getattr(e, '_originating_controller', None), [], e, {}, func(e))

    return wrapper


@browsable_exception
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

@browsable_exception
def serialize_no_data(e):
    from ..tools import ViewCache
    cherrypy.response.headers['Content-Type'] = 'application/json'
    cherrypy.response.status = getattr(e, 'status', 400)
    return {'status': ViewCache.VALUE_NONE, 'value': None}


@partial(Tool, 'before_handler', name='dashboard_exception_handler')
def dashboard_exception_handler(_handle_rbd_error=False,
                                _handle_rados_error=None,
                                _handle_send_command_error=None):

    def inner(*args, **kwargs):
        handler = innerfunc

        try:
            if _handle_rbd_error:
                handler = _c2d(handle_rbd_error)(handler)

            handler = _c2d(handle_rados_error, _handle_rados_error)(handler)

            if _handle_send_command_error:
                handler = _c2d(handle_send_command_error, _handle_send_command_error)(handler)

            return handler(*args, **kwargs)
        # Don't catch cherrypy.* Exceptions.
        except ViewCacheNoDataException as e:
            logger.exception('dashboard_exception_handler')
            return serialize_no_data(e)
        except DashboardException as e:
            logger.exception('dashboard_exception_handler')
            return serialize_dashboard_exception(e)
        except Exception as e:
            from ..tools import browsable_api_enabled
            if not browsable_api_enabled():
                raise
            logger.exception('dashboard_exception_handler')
            return browsable_exception(lambda e: {})(e)

    innerfunc = cherrypy.serving.request.handler
    cherrypy.serving.request.handler = inner


@contextmanager
def handle_rbd_error():
    try:
        yield
    except rbd.OSError as e:
        raise DashboardException(e, component='rbd')
    except rbd.Error as e:
        raise DashboardException(e, component='rbd', code=e.__class__.__name__)


def set_handle_rbd_error():
    """
    Meant to be used as a decorator.

    >>> @cherrypy.expose
    ... @set_handle_rbd_error
    ... def error_send_command(self):
    ...     pass

    Instead of calling `dashboard_exception_handler` from above, it just enables the tool
    """
    return dashboard_exception_handler(_handle_rbd_error=True)


@contextmanager
def handle_rados_error(component):
    try:
        yield
    except rados.OSError as e:
        raise DashboardException(e, component=component)
    except rados.Error as e:
        raise DashboardException(e, component=component, code=e.__class__.__name__)


def set_handle_rados_error(component):
    """
    Meant to be used as a decorator.

    >>> @cherrypy.expose
    ... @set_handle_rados_error('foo')
    ... def error_send_command(self):
    ...     raise SendCommandError('hi', 'prefix', {}, -42)

    Instead of calling `dashboard_exception_handler` from above, it just enables the tool
    """
    return dashboard_exception_handler(_handle_rados_error=component)


@contextmanager
def handle_send_command_error(component):
    try:
        yield
    except SendCommandError as e:
        raise DashboardException(e, component=component)


def set_handle_send_command_error(component):
    """
    Meant to be used as a decorator.

    >>> @cherrypy.expose
    ... @set_handle_send_command_error('foo')
    ... def error_send_command(self):
    ...     pass

    Instead of calling `dashboard_exception_handler` from above, it just enables the tool
    """
    return dashboard_exception_handler(_handle_send_command_error=component)


def _c2d(my_contextmanager, *cargs, **ckwargs):
    """Converts a contextmanager into a decorator. Only needed for Python 2"""
    def decorator(f):
        def wrapper(*args, **kwargs):
            with my_contextmanager(*cargs, **ckwargs):
                return f(*args, **kwargs)

        return wrapper
    return decorator
