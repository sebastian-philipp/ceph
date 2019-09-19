import errno


class Error(Exception):
    """ `Error` class, derived from `Exception` """
    def __init__(self, message, errno=None):
        super(Exception, self).__init__(message)
        self.errno = errno

    def __str__(self):
        msg = super(Exception, self).__str__()
        if self.errno is None:
            return msg
        return '[errno {0}] {1}'.format(self.errno, msg)

class InvalidArgumentError(Error):
    pass

class OSError(Error):
    """ `OSError` class, derived from `Error` """
    pass

class InterruptedOrTimeoutError(OSError):
    """ `InterruptedOrTimeoutError` class, derived from `OSError` """
    pass


class PermissionError(OSError):
    """ `PermissionError` class, derived from `OSError` """
    pass


class PermissionDeniedError(OSError):
    """ deal with EACCES related. """
    pass


class ObjectNotFound(OSError):
    """ `ObjectNotFound` class, derived from `OSError` """
    pass


class NoData(OSError):
    """ `NoData` class, derived from `OSError` """
    pass


class ObjectExists(OSError):
    """ `ObjectExists` class, derived from `OSError` """
    pass


class ObjectBusy(OSError):
    """ `ObjectBusy` class, derived from `IOError` """
    pass


class IOError(OSError):
    """ `ObjectBusy` class, derived from `OSError` """
    pass


class NoSpace(OSError):
    """ `NoSpace` class, derived from `OSError` """
    pass


class RadosStateError(Error):
    """ `RadosStateError` class, derived from `Error` """
    pass


class IoctxStateError(Error):
    """ `IoctxStateError` class, derived from `Error` """
    pass


class ObjectStateError(Error):
    """ `ObjectStateError` class, derived from `Error` """
    pass


class LogicError(Error):
    """ `` class, derived from `Error` """
    pass


class TimedOut(OSError):
    """ `TimedOut` class, derived from `OSError` """
    pass

errno_to_exception = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.EBUSY     : ObjectBusy,
        errno.ENODATA   : NoData,
        errno.EINTR     : InterruptedOrTimeoutError,
        errno.ETIMEDOUT : TimedOut,
        errno.EACCES    : PermissionDeniedError,
        errno.EINVAL    : InvalidArgumentError,
    }


def make_ex(ret, msg):
    """
    Translate a librados return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """
    ret = abs(ret)
    return errno_to_exception.get(ret, OSError)(msg, errno=ret)
