
"""
ceph-mgr orchestrator interface

Please see the ceph-mgr module developer's guide for more information.
"""
import functools
import logging
import sys
import time
from collections import namedtuple
from functools import wraps
import uuid
import string
import random
import datetime
import copy
import re
import six

from ceph.deployment import inventory

from mgr_module import MgrModule, PersistentStoreDict
from mgr_util import format_bytes

try:
    from ceph.deployment.drive_group import DriveGroupSpec
    from typing import TypeVar, Generic, List, Optional, Union, Tuple, Iterator, Callable, Any, Type

except ImportError:
    pass
    #T, G = object, object

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')
G = Generic[T]
Promises = TypeVar('Promises', bound='_Promise')
Completions = TypeVar('Completions', bound='Completion')



def parse_host_specs(host, require_network=True):
    """
    Split host into host, network, and (optional) daemon name parts.  The network
    part can be an IP, CIDR, or ceph addrvec like '[v2:1.2.3.4:3300,v1:1.2.3.4:6789]'.
    e.g.,
      "myhost"
      "myhost=name"
      "myhost:1.2.3.4"
      "myhost:1.2.3.4=name"
      "myhost:1.2.3.0/24"
      "myhost:1.2.3.0/24=name"
      "myhost:[v2:1.2.3.4:3000]=name"
      "myhost:[v2:1.2.3.4:3000,v1:1.2.3.4:6789]=name"
    """
    # Matches from start to : or = or until end of string
    host_re = r'^(.*?)(:|=|$)'
    # Matches from : to = or until end of string
    ip_re = r':(.*?)(=|$)'
    # Matches from = to end of string
    name_re = r'=(.*?)$'

    from collections import namedtuple
    HostSpec = namedtuple('HostSpec', ['hostname', 'network', 'name'])
    # assign defaults
    host_spec = HostSpec('', '', '')

    match_host = re.search(host_re, host)
    if match_host:
        host_spec = host_spec._replace(hostname=match_host.group(1))

    name_match = re.search(name_re, host)
    if name_match:
        host_spec = host_spec._replace(name=name_match.group(1))

    ip_match = re.search(ip_re, host)
    if ip_match:
        host_spec = host_spec._replace(network=ip_match.group(1))

    if not require_network:
        return host_spec

    from ipaddress import ip_network, ip_address
    networks = list()
    network = host_spec.network
    # in case we have [v2:1.2.3.4:3000,v1:1.2.3.4:6478]
    if ',' in network:
        networks = [x for x in network.split(',')]
    else:
        networks.append(network)
    for network in networks:
        # only if we have versioned network configs
        if network.startswith('v') or network.startswith('[v'):
            network = network.split(':')[1]
        try:
            # if subnets are defined, also verify the validity
            if '/' in network:
                ip_network(six.text_type(network))
            else:
                ip_address(six.text_type(network))
        except ValueError as e:
            # logging?
            raise e

    return host_spec


class OrchestratorError(Exception):
    """
    General orchestrator specific error.

    Used for deployment, configuration or user errors.

    It's not intended for programming errors or orchestrator internal errors.
    """


class NoOrchestrator(OrchestratorError):
    """
    No orchestrator in configured.
    """
    def __init__(self, msg="No orchestrator configured (try `ceph orchestrator set backend`)"):
        super(NoOrchestrator, self).__init__(msg)


class OrchestratorValidationError(OrchestratorError):
    """
    Raised when an orchestrator doesn't support a specific feature.
    """

def _no_result():
    return object()


class _Promise(Generic[T]):
    """
    A completion may need multiple promises to be fulfilled. `_Promise` is one
    step.

    Typically ``Orchestrator`` implementations inherit from this class to
    build their own way of finishing a step to fulfil a future.

    They are not exposed in the orchestrator interface and can be seen as a
    helper to build orchestrator modules.
    """
    INITIALIZED = 1  # We have a parent completion and a next completion
    FINISHED = 2  # we have a final result

    NO_RESULT = _no_result()  # type: None

    def __init__(self,
                 _first_promise=None,  # type: Optional["_Promise[V]"]
                 value=NO_RESULT,  # type: Optional[T]
                 on_complete=None    # type: Optional[Callable[[T], Union[U, _Promise[U]]]]
                 ):
        self._on_complete = on_complete
        self._next_promise = None  # type: Optional[_Promise[U]]

        self._state = self.INITIALIZED
        self._exception = None  # type: Optional[Exception]

        # Value of this _Promise. may be an intermediate result.
        self._value = value

        # _Promise is not a continuation monad, as `_result` is of type
        # T instead of (T -> r) -> r. Therefore we need to store the first promise here.
        self._first_promise = _first_promise or self  # type: 'Completion'

    def __repr__(self):
        name = getattr(self._on_complete, '__name__', '??') if self._on_complete else 'None'
        val = repr(self._value) if self._value is not self.NO_RESULT else 'NA'
        return '{}(_s={}, val={}, id={}, name={}, pr={}, _next={})'.format(
            self.__class__, self._state, val, id(self), name, getattr(next, '_progress_reference', 'NA'), repr(self._next_promise)
        )

    def then(self, on_complete):
        # type: (Promises, Callable[[T], Union[U, _Promise[U]]]) -> Promises[U]
        """
        Call ``on_complete`` as soon as this promise is finalized.
        """
        assert self._state is self.INITIALIZED
        if self._on_complete is not None:
            assert self._next_promise is None
            self._set_next_promise(self.__class__(
                _first_promise=self._first_promise,
                on_complete=on_complete
            ))
            return self._next_promise

        else:
            self._on_complete = on_complete
            self._set_next_promise(self.__class__(_first_promise=self._first_promise))
            return self._next_promise

    def _set_next_promise(self, next):
        # type: (_Promise[U]) -> None
        assert self is not next
        assert self._state is self.INITIALIZED

        self._next_promise = next
        assert self._next_promise is not None
        for p in iter(self._next_promise):
            p._first_promise = self._first_promise

    def finalize(self, value=NO_RESULT):
        # type: (Optional[T]) -> None
        """
        Sets this promise to complete.

        Orchestrators may choose to use this helper function.

        :param value: new value.
        """
        assert self._state is self.INITIALIZED

        if value is not self.NO_RESULT:
            self._value = value
        assert self._value is not self.NO_RESULT

        if self._on_complete:
            try:
                next_result = self._on_complete(self._value)
            except Exception as e:
                self.fail(e)
                return
        else:
            next_result = self._value

        if isinstance(next_result, _Promise):
            # hack: _Promise is not a continuation monad.
            next_result = next_result._first_promise  # type: ignore
            assert next_result not in self, repr(self._first_promise) + repr(next_result)
            assert self not in next_result
            next_result._append_promise(self._next_promise)
            self._set_next_promise(next_result)
            if self._next_promise._value is self.NO_RESULT:
                self._next_promise._value = self._value
        else:
            # simple map. simply forward
            if self._next_promise:
                self._next_promise._value = next_result
            else:
                # Hack: next_result is of type U, _value is of type T
                self._value = next_result  # type: ignore
        self._state = self.FINISHED
        logger.debug('finalized {}'.format(repr(self)))
        self.propagate_to_next()

    def propagate_to_next(self):
        assert self._state is self.FINISHED
        if self._next_promise:
            self._next_promise.finalize()

    def fail(self, e):
        # type: (Exception) -> None
        """
        Sets the whole completion to be faild with this exception and end the
        evaluation.
        """
        assert self._state is self.INITIALIZED
        logger.exception('_Promise failed')
        self._exception = e
        self._value = 'exception'
        if self._next_promise:
            self._next_promise.fail(e)
        self._state = self.FINISHED

    def __contains__(self, item):
        return any(item is p for p in iter(self._first_promise))

    def __iter__(self):
        yield self
        elem = self._next_promise
        while elem is not None:
            yield elem
            elem = elem._next_promise

    def _append_promise(self, other):
        if other is not None:
            assert self not in other
            assert other not in self
            self._last_promise()._set_next_promise(other)

    def _last_promise(self):
        # type: () -> _Promise
        return list(iter(self))[-1]


class ProgressReference(object):
    def __init__(self,
                 message,  # type: str
                 mgr,
                 completion=None  # type: Optional[Callable[[], Completion[float]]]
                ):
        """
        ProgressReference can be used within Completions:

        +---------------+      +---------------------------------+
        |               | then |                                 |
        | My Completion | +--> | on_complete=ProgressReference() |
        |               |      |                                 |
        +---------------+      +---------------------------------+

        """
        super(ProgressReference, self).__init__()
        self.progress_id = str(uuid.uuid4())
        self.message = message
        self.mgr = mgr

        #: The completion can already have a result, before the write
        #: operation is effective. progress == 1 means, the services are
        #: created / removed.
        self.completion = completion  # type: Optional[Callable[[], Completion[float]]]

        #: if a orchestrator module can provide a more detailed
        #: progress information, it needs to also call ``progress.update()``.
        self.progress = 0.0

        self._completion_has_result = False
        self.mgr.all_progress_references.append(self)

    def __str__(self):
        """
        ``__str__()`` is used for determining the message for progress events.
        """
        return self.message or super(ProgressReference, self).__str__()

    def __call__(self, arg):
        self._completion_has_result = True
        if self.progress == 0.0:
            self.progress = 0.5
        return arg

    @property
    def progress(self):
        return self._progress

    @progress.setter
    def progress(self, progress):
        self._progress = progress
        try:
            if self.effective:
                self.mgr.remote("progress", "complete", self.progress_id)
                self.mgr.all_progress_references = [p for p in self.mgr.all_progress_references if p is not self]
            else:
                self.mgr.remote("progress", "update", self.progress_id, self.message,
                                progress,
                                [("origin", "orchestrator")])
        except ImportError:
            # If the progress module is disabled that's fine,
            # they just won't see the output.
            pass

    @property
    def effective(self):
        return self.progress == 1 and self._completion_has_result

    def update(self):
        def run(progress):
            self.progress = progress
        if self.completion:
            c = self.completion().then(run)
            self.mgr.process([c._first_promise])
        else:
            self.progress = 1

    def fail(self):
        self._completion_has_result = True
        self.progress = 1

class Completion(_Promise[T]):
    """
    Combines multiple promises into one overall operation.

    :ivar exception: Holds an exception object, if the completion errored.

    """
    def __init__(self,
                 _first_promise=None,  # type: Optional["Completion[V]"]
                 value=_Promise.NO_RESULT,  # type: Optional[T]
                 on_complete=None  # type: Optional[Callable[[T], Union[U, Completion[U]]]]
                 ):
        super(Completion, self).__init__(_first_promise, value, on_complete)

    @property
    def _progress_reference(self):
        # type: () -> Optional[ProgressReference]
        if hasattr(self._on_complete, 'progress_id'):
            return self._on_complete
        return None

    @property
    def progress_reference(self):
        # type: () -> Optional[ProgressReference]
        """
        ProgressReference. Marks this completion
        as a write completeion.
        """

        references = [c._progress_reference for c in iter(self) if c._progress_reference is not None]
        if references:
            assert len(references) == 1
            return references[0]
        return None

    @classmethod
    def with_progress(cls,  # type: Completions[T]
                      message,  # type: str
                      mgr,
                      _first_promise=None,  # type: Optional["Completions[V]"]
                      value=_Promise.NO_RESULT,  # type: Optional[T]
                      on_complete=None,  # type: Optional[Callable[[T], Union[U, Completions[U]]]]
                      calc_percent=None  # type: Optional[Callable[[], Completions[float]]]
                      ):
        # type: (...) -> Completions[T]

        c = cls(
            _first_promise=_first_promise,
            value=value,
            on_complete=on_complete
        ).then(
            on_complete=ProgressReference(
                message=message,
                mgr=mgr,
                completion=calc_percent
            )
        )
        return c._first_promise

    def fail(self, e):
        super(Completion, self).fail(e)
        if self._progress_reference:
            self._progress_reference.fail()

    @property
    def result(self):
        """
        The result of the operation that we were waited
        for.  Only valid after calling Orchestrator.process() on this
        completion.
        """
        last = self._last_promise()
        assert last._state == _Promise.FINISHED
        return last._value

    def result_str(self):
        """Force a string."""
        if self.result is None:
            return ''
        return str(self.result)

    @property
    def exception(self):
        # type: () -> Optional[Exception]
        return self._last_promise()._exception

    @property
    def has_result(self):
        # type: () -> bool
        """
        Has the operation already a result?

        For Write operations, it can already have a
        result, if the orchestrator's configuration is
        persistently written. Typically this would
        indicate that an update had been written to
        a manifest, but that the update had not
        necessarily been pushed out to the cluster.

        :return:
        """
        return self._last_promise()._state == _Promise.FINISHED

    @property
    def is_errored(self):
        # type: () -> bool
        """
        Has the completion failed. Default implementation looks for
        self.exception. Can be overwritten.
        """
        return self.exception is not None

    @property
    def needs_result(self):
        # type: () -> bool
        """
        Could the external operation be deemed as complete,
        or should we wait?
        We must wait for a read operation only if it is not complete.
        """
        return not self.is_errored and not self.has_result

    @property
    def is_finished(self):
        # type: () -> bool
        """
        Could the external operation be deemed as complete,
        or should we wait?
        We must wait for a read operation only if it is not complete.
        """
        return self.is_errored or (self.has_result)


def raise_if_exception(c):
    # type: (Completion) -> None
    """
    :raises OrchestratorError: Some user error or a config error.
    :raises Exception: Some internal error
    """
    def copy_to_this_subinterpreter(r_obj):
        # This is something like `return pickle.loads(pickle.dumps(r_obj))`
        # Without importing anything.
        r_cls = r_obj.__class__
        if r_cls.__module__ in ('__builtin__', 'builtins'):
            return r_obj
        my_cls = getattr(sys.modules[r_cls.__module__], r_cls.__name__)
        if id(my_cls) == id(r_cls):
            return r_obj
        if hasattr(r_obj, '__reduce__'):
            reduce_tuple = r_obj.__reduce__()
            if len(reduce_tuple) >= 2:
                return my_cls(*[copy_to_this_subinterpreter(a) for a in reduce_tuple[1]])
        my_obj = my_cls.__new__(my_cls)
        for k,v in r_obj.__dict__.items():
            setattr(my_obj, k, copy_to_this_subinterpreter(v))
        return my_obj

    if c.exception is not None:
        try:
            e = copy_to_this_subinterpreter(c.exception)
        except (KeyError, AttributeError):
            raise Exception(str(c.exception))
        raise e


class TrivialReadCompletion(Completion[T]):
    """
    This is the trivial completion simply wrapping a result.
    """
    def __init__(self, result):
        super(TrivialReadCompletion, self).__init__()
        self._result = result


def _hide_in_features(f):
    f._hide_in_features = True
    return f


class Orchestrator(object):
    """
    Calls in this class may do long running remote operations, with time
    periods ranging from network latencies to package install latencies and large
    internet downloads.  For that reason, all are asynchronous, and return
    ``Completion`` objects.

    Methods should only return the completion and not directly execute
    anything, like network calls. Otherwise the purpose of
    those completions is defeated.

    Implementations are not required to start work on an operation until
    the caller waits on the relevant Completion objects.  Callers making
    multiple updates should not wait on Completions until they're done
    sending operations: this enables implementations to batch up a series
    of updates when wait() is called on a set of Completion objects.

    Implementations are encouraged to keep reasonably fresh caches of
    the status of the system: it is better to serve a stale-but-recent
    result read of e.g. device inventory than it is to keep the caller waiting
    while you scan hosts every time.
    """

    @_hide_in_features
    def is_orchestrator_module(self):
        """
        Enable other modules to interrogate this module to discover
        whether it's usable as an orchestrator module.

        Subclasses do not need to override this.
        """
        return True

    @_hide_in_features
    def available(self):
        # type: () -> Tuple[bool, str]
        """
        Report whether we can talk to the orchestrator.  This is the
        place to give the user a meaningful message if the orchestrator
        isn't running or can't be contacted.

        This method may be called frequently (e.g. every page load
        to conditionally display a warning banner), so make sure it's
        not too expensive.  It's okay to give a slightly stale status
        (e.g. based on a periodic background ping of the orchestrator)
        if that's necessary to make this method fast.

        .. note::
            `True` doesn't mean that the desired functionality
            is actually available in the orchestrator. I.e. this
            won't work as expected::

                >>> if OrchestratorClientMixin().available()[0]:  # wrong.
                ...     OrchestratorClientMixin().get_hosts()

        :return: two-tuple of boolean, string
        """
        raise NotImplementedError()

    @_hide_in_features
    def process(self, completions):
        # type: (List[Completion]) -> None
        """
        Given a list of Completion instances, process any which are
        incomplete.

        Callers should inspect the detail of each completion to identify
        partial completion/progress information, and present that information
        to the user.

        This method should not block, as this would make it slow to query
        a status, while other long running operations are in progress.
        """
        raise NotImplementedError()

    @_hide_in_features
    def get_feature_set(self):
        """Describes which methods this orchestrator implements

        .. note::
            `True` doesn't mean that the desired functionality
            is actually possible in the orchestrator. I.e. this
            won't work as expected::

                >>> api = OrchestratorClientMixin()
                ... if api.get_feature_set()['get_hosts']['available']:  # wrong.
                ...     api.get_hosts()

            It's better to ask for forgiveness instead::

                >>> try:
                ...     OrchestratorClientMixin().get_hosts()
                ... except (OrchestratorError, NotImplementedError):
                ...     ...

        :returns: Dict of API method names to ``{'available': True or False}``
        """
        module = self.__class__
        features = {a: {'available': getattr(Orchestrator, a, None) != getattr(module, a)}
                    for a in Orchestrator.__dict__
                    if not a.startswith('_') and not getattr(getattr(Orchestrator, a), '_hide_in_features', False)
                    }
        return features

    def add_host(self, host):
        # type: (str) -> Completion
        """
        Add a host to the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def remove_host(self, host):
        # type: (str) -> Completion
        """
        Remove a host from the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def get_hosts(self):
        # type: () -> Completion[List[InventoryNode]]
        """
        Report the hosts in the cluster.

        The default implementation is extra slow.

        :return: list of InventoryNodes
        """
        return self.get_inventory()

    def get_inventory(self, node_filter=None, refresh=False):
        # type: (InventoryFilter, bool) -> Completion[List[InventoryNode]]
        """
        Returns something that was created by `ceph-volume inventory`.

        :return: list of InventoryNode
        """
        raise NotImplementedError()

    def describe_service(self, service_type=None, service_id=None, node_name=None, refresh=False):
        # type: (Optional[str], Optional[str], Optional[str], bool) -> Completion[List[ServiceDescription]]
        """
        Describe a service (of any kind) that is already configured in
        the orchestrator.  For example, when viewing an OSD in the dashboard
        we might like to also display information about the orchestrator's
        view of the service (like the kubernetes pod ID).

        When viewing a CephFS filesystem in the dashboard, we would use this
        to display the pods being currently run for MDS daemons.

        :return: list of ServiceDescription objects.
        """
        raise NotImplementedError()

    def service_action(self, action, service_type, service_name=None, service_id=None):
        # type: (str, str, str, str) -> Completion
        """
        Perform an action (start/stop/reload) on a service.

        Either service_name or service_id must be specified:

        * If using service_name, perform the action on that entire logical
          service (i.e. all daemons providing that named service).
        * If using service_id, perform the action on a single specific daemon
          instance.

        :param action: one of "start", "stop", "reload", "restart", "redeploy"
        :param service_type: e.g. "mds", "rgw", ...
        :param service_name: name of logical service ("cephfs", "us-east", ...)
        :param service_id: service daemon instance (usually a short hostname)
        :rtype: Completion
        """
        #assert action in ["start", "stop", "reload, "restart", "redeploy"]
        #assert service_name or service_id
        #assert not (service_name and service_id)
        raise NotImplementedError()

    def create_osds(self, drive_group):
        # type: (DriveGroupSpec) -> Completion
        """
        Create one or more OSDs within a single Drive Group.

        The principal argument here is the drive_group member
        of OsdSpec: other fields are advisory/extensible for any
        finer-grained OSD feature enablement (choice of backing store,
        compression/encryption, etc).

        :param drive_group: DriveGroupSpec
        :param all_hosts: TODO, this is required because the orchestrator methods are not composable
                Probably this parameter can be easily removed because each orchestrator can use
                the "get_inventory" method and the "drive_group.host_pattern" attribute
                to obtain the list of hosts where to apply the operation
        """
        raise NotImplementedError()

    def remove_osds(self, osd_ids):
        # type: (List[str]) -> Completion
        """
        :param osd_ids: list of OSD IDs
        :param destroy: marks the OSD as being destroyed. See :ref:`orchestrator-osd-replace`

        Note that this can only remove OSDs that were successfully
        created (i.e. got an OSD ID).
        """
        raise NotImplementedError()

    def blink_device_light(self, ident_fault, on, locations):
        # type: (str, bool, List[DeviceLightLoc]) -> WriteCompletion
        """
        Instructs the orchestrator to enable or disable either the ident or the fault LED.

        :param ident_fault: either ``"ident"`` or ``"fault"``
        :param on: ``True`` = on.
        :param locations: See :class:`orchestrator.DeviceLightLoc`
        """
        raise NotImplementedError()

    def update_mgrs(self, num, hosts):
        # type: (int, List[str]) -> Completion
        """
        Update the number of cluster managers.

        :param num: requested number of managers.
        :param hosts: list of hosts (optional)
        """
        raise NotImplementedError()

    def update_mons(self, num, hosts):
        # type: (int, List[Tuple[str,str]]) -> Completion
        """
        Update the number of cluster monitors.

        :param num: requested number of monitors.
        :param hosts: list of hosts + network + name (optional)
        """
        raise NotImplementedError()

    def add_mds(self, spec):
        # type: (StatelessServiceSpec) -> Completion
        """Create a new MDS cluster"""
        raise NotImplementedError()

    def remove_mds(self, name):
        # type: (str) -> Completion
        """Remove an MDS cluster"""
        raise NotImplementedError()

    def update_mds(self, spec):
        # type: (StatelessServiceSpec) -> Completion
        """
        Update / redeploy existing MDS cluster
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    def add_rbd_mirror(self, spec):
        # type: (StatelessServiceSpec) -> WriteCompletion
        """Create rbd-mirror cluster"""
        raise NotImplementedError()

    def remove_rbd_mirror(self):
        # type: (str) -> WriteCompletion
        """Remove rbd-mirror cluster"""
        raise NotImplementedError()

    def update_rbd_mirror(self, spec):
        # type: (StatelessServiceSpec) -> WriteCompletion
        """
        Update / redeploy rbd-mirror cluster
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    def add_nfs(self, spec):
        # type: (NFSServiceSpec) -> Completion
        """Create a new MDS cluster"""
        raise NotImplementedError()

    def remove_nfs(self, name):
        # type: (str) -> Completion
        """Remove a NFS cluster"""
        raise NotImplementedError()

    def update_nfs(self, spec):
        # type: (NFSServiceSpec) -> Completion
        """
        Update / redeploy existing NFS cluster
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    def add_rgw(self, spec):
        # type: (RGWSpec) -> Completion
        """Create a new MDS zone"""
        raise NotImplementedError()

    def remove_rgw(self, zone):
        # type: (str) -> Completion
        """Remove a RGW zone"""
        raise NotImplementedError()

    def update_rgw(self, spec):
        # type: (StatelessServiceSpec) -> Completion
        """
        Update / redeploy existing RGW zone
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_start(self, upgrade_spec):
        # type: (UpgradeSpec) -> Completion
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_status(self):
        # type: () -> Completion[UpgradeStatusSpec]
        """
        If an upgrade is currently underway, report on where
        we are in the process, or if some error has occurred.

        :return: UpgradeStatusSpec instance
        """
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_available(self):
        # type: () -> Completion[List[str]]
        """
        Report on what versions are available to upgrade to

        :return: List of strings
        """
        raise NotImplementedError()


class UpgradeSpec(object):
    # Request to orchestrator to initiate an upgrade to a particular
    # version of Ceph
    def __init__(self):
        self.target_version = None


class UpgradeStatusSpec(object):
    # Orchestrator's report on what's going on with any ongoing upgrade
    def __init__(self):
        self.in_progress = False  # Is an upgrade underway?
        self.services_complete = []  # Which daemon types are fully updated?
        self.message = ""  # Freeform description


class PlacementSpec(object):
    """
    For APIs that need to specify a node subset
    """
    def __init__(self, label=None, nodes=[]):
        self.label = label

        self.nodes = [parse_host_specs(x, require_network=False) for x in nodes]

def handle_type_error(method):
    @wraps(method)
    def inner(cls, *args, **kwargs):
        try:
            return method(cls, *args, **kwargs)
        except TypeError as e:
            error_msg = '{}: {}'.format(cls.__name__, e)
        raise OrchestratorValidationError(error_msg)
    return inner


class ServiceDescription(object):
    """
    For responding to queries about the status of a particular service,
    stateful or stateless.

    This is not about health or performance monitoring of services: it's
    about letting the orchestrator tell Ceph whether and where a
    service is scheduled in the cluster.  When an orchestrator tells
    Ceph "it's running on node123", that's not a promise that the process
    is literally up this second, it's a description of where the orchestrator
    has decided the service should run.
    """

    def __init__(self, nodename=None, container_id=None, service=None, service_instance=None,
                 service_type=None, version=None, rados_config_location=None,
                 service_url=None, status=None, status_desc=None):
        # Node is at the same granularity as InventoryNode
        self.nodename = nodename

        # Not everyone runs in containers, but enough people do to
        # justify having this field here.
        self.container_id = container_id

        # Some services can be deployed in groups. For example, mds's can
        # have an active and standby daemons, and nfs-ganesha can run daemons
        # in parallel. This tag refers to a group of daemons as a whole.
        #
        # For instance, a cluster of mds' all service the same fs, and they
        # will all have the same service value (which may be the
        # Filesystem name in the FSMap).
        #
        # Single-instance services should leave this set to None
        self.service = service

        # The orchestrator will have picked some names for daemons,
        # typically either based on hostnames or on pod names.
        # This is the <foo> in mds.<foo>, the ID that will appear
        # in the FSMap/ServiceMap.
        self.service_instance = service_instance

        # The type of service (osd, mon, mgr, etc.)
        self.service_type = service_type

        # Service version that was deployed
        self.version = version

        # Location of the service configuration when stored in rados
        # object. Format: "rados://<pool>/[<namespace/>]<object>"
        self.rados_config_location = rados_config_location

        # If the service exposes REST-like API, this attribute should hold
        # the URL.
        self.service_url = service_url

        # Service status: -1 error, 0 stopped, 1 running
        self.status = status

        # Service status description when status == -1.
        self.status_desc = status_desc

    def name(self):
        if self.service_instance:
            return '%s.%s' % (self.service_type, self.service_instance)
        return self.service_type

    def __repr__(self):
        return "<ServiceDescription>({n_name}:{s_type})".format(n_name=self.nodename,
                                                                  s_type=self.name())

    def to_json(self):
        out = {
            'nodename': self.nodename,
            'container_id': self.container_id,
            'service': self.service,
            'service_instance': self.service_instance,
            'service_type': self.service_type,
            'version': self.version,
            'rados_config_location': self.rados_config_location,
            'service_url': self.service_url,
            'status': self.status,
            'status_desc': self.status_desc,
        }
        return {k: v for (k, v) in out.items() if v is not None}

    @classmethod
    @handle_type_error
    def from_json(cls, data):
        return cls(**data)


class StatelessServiceSpec(object):
    # Request to orchestrator for a group of stateless services
    # such as MDS, RGW, nfs gateway, iscsi gateway
    """
    Details of stateless service creation.

    Request to orchestrator for a group of stateless services
    such as MDS, RGW or iscsi gateway
    """
    # This structure is supposed to be enough information to
    # start the services.

    def __init__(self, name, placement=None, count=None):
        self.placement = PlacementSpec() if placement is None else placement

        #: Give this set of statelss services a name: typically it would
        #: be the name of a CephFS filesystem, RGW zone, etc.  Must be unique
        #: within one ceph cluster.
        self.name = name

        #: Count of service instances
        self.count = 1 if count is None else count

    def validate_add(self):
        if not self.name:
            raise OrchestratorValidationError('Cannot add Service: Name required')


class NFSServiceSpec(StatelessServiceSpec):
    def __init__(self, name, pool=None, namespace=None, count=1, placement=None):
        super(NFSServiceSpec, self).__init__(name, placement, count)

        #: RADOS pool where NFS client recovery data is stored.
        self.pool = pool

        #: RADOS namespace where NFS client recovery data is stored in the pool.
        self.namespace = namespace

    def validate_add(self):
        super(NFSServiceSpec, self).validate_add()

        if not self.pool:
            raise OrchestratorValidationError('Cannot add NFS: No Pool specified')


class RGWSpec(StatelessServiceSpec):
    """
    Settings to configure a (multisite) Ceph RGW

    """
    def __init__(self,
                 rgw_zone,  # type: str
                 placement=None,
                 hosts=None,  # type: Optional[List[str]]
                 rgw_multisite=None,  # type: Optional[bool]
                 rgw_zonemaster=None,  # type: Optional[bool]
                 rgw_zonesecondary=None,  # type: Optional[bool]
                 rgw_multisite_proto=None,  # type: Optional[str]
                 rgw_frontend_port=None,  # type: Optional[int]
                 rgw_zonegroup=None,  # type: Optional[str]
                 rgw_zone_user=None,  # type: Optional[str]
                 rgw_realm=None,  # type: Optional[str]
                 system_access_key=None,  # type: Optional[str]
                 system_secret_key=None,  # type: Optional[str]
                 count=None  # type: Optional[int]
                 ):
        # Regarding default values. Ansible has a `set_rgwspec_defaults` that sets
        # default values that makes sense for Ansible. Rook has default values implemented
        # in Rook itself. Thus we don't set any defaults here in this class.

        super(RGWSpec, self).__init__(name=rgw_zone, count=count,
                                      placement=placement)

        #: List of hosts where RGWs should run. Not for Rook.
        if hosts:
            self.placement.hosts = hosts

        #: is multisite
        self.rgw_multisite = rgw_multisite
        self.rgw_zonemaster = rgw_zonemaster
        self.rgw_zonesecondary = rgw_zonesecondary
        self.rgw_multisite_proto = rgw_multisite_proto
        self.rgw_frontend_port = rgw_frontend_port

        self.rgw_zonegroup = rgw_zonegroup
        self.rgw_zone_user = rgw_zone_user
        self.rgw_realm = rgw_realm

        self.system_access_key = system_access_key
        self.system_secret_key = system_secret_key

    @property
    def rgw_multisite_endpoint_addr(self):
        """Returns the first host. Not supported for Rook."""
        return self.hosts[0]

    @property
    def rgw_multisite_endpoints_list(self):
        return ",".join(["{}://{}:{}".format(self.rgw_multisite_proto,
                             host,
                             self.rgw_frontend_port) for host in self.hosts])

    def genkey(self, nchars):
        """ Returns a random string of nchars

        :nchars : Length of the returned string
        """
        # TODO Python 3: use Secrets module instead.

        return ''.join(random.choice(string.ascii_uppercase +
                                     string.ascii_lowercase +
                                     string.digits) for _ in range(nchars))

    @classmethod
    def from_json(cls, json_rgw_spec):
        # type: (dict) -> RGWSpec
        """
        Initialize 'RGWSpec' object data from a json structure
        :param json_rgw_spec: A valid dict with a the RGW settings
        """
        # TODO: also add PlacementSpec(**json_rgw_spec['placement'])
        args = {k:v for k, v in json_rgw_spec.items()}
        return RGWSpec(**args)


class InventoryFilter(object):
    """
    When fetching inventory, use this filter to avoid unnecessarily
    scanning the whole estate.

    Typical use: filter by node when presenting UI workflow for configuring
                 a particular server.
                 filter by label when not all of estate is Ceph servers,
                 and we want to only learn about the Ceph servers.
                 filter by label when we are interested particularly
                 in e.g. OSD servers.

    """
    def __init__(self, labels=None, nodes=None):
        # type: (List[str], List[str]) -> None
        self.labels = labels  # Optional: get info about nodes matching labels
        self.nodes = nodes  # Optional: get info about certain named nodes only




class InventoryNode(object):
    """
    When fetching inventory, all Devices are groups inside of an
    InventoryNode.
    """
    def __init__(self, name, devices=None):
        # type: (str, inventory.Devices) -> None
        if devices is None:
            devices = inventory.Devices([])
        assert isinstance(devices, inventory.Devices)

        self.name = name  # unique within cluster.  For example a hostname.
        self.devices = devices

    def to_json(self):
        return {'name': self.name, 'devices': self.devices.to_json()}

    @classmethod
    def from_json(cls, data):
        try:
            _data = copy.deepcopy(data)
            name = _data.pop('name')
            devices = inventory.Devices.from_json(_data.pop('devices'))
            if _data:
                error_msg = 'Unknown key(s) in Inventory: {}'.format(','.join(_data.keys()))
                raise OrchestratorValidationError(error_msg)
            return cls(name, devices)
        except KeyError as e:
            error_msg = '{} is required for {}'.format(e, cls.__name__)
            raise OrchestratorValidationError(error_msg)
        except TypeError as e:
            raise OrchestratorValidationError('Failed to read inventory: {}'.format(e))


    @classmethod
    def from_nested_items(cls, hosts):
        devs = inventory.Devices.from_json
        return [cls(item[0], devs(item[1].data)) for item in hosts]

    def __repr__(self):
        return "<InventoryNode>({name})".format(name=self.name)

    @staticmethod
    def get_host_names(nodes):
        # type: (List[InventoryNode]) -> List[str]
        return [node.name for node in nodes]


class DeviceLightLoc(namedtuple('DeviceLightLoc', ['host', 'dev'])):
    """
    Describes a specific device on a specific host. Used for enabling or disabling LEDs
    on devices.

    hostname as in :func:`orchestrator.Orchestrator.get_hosts`

    device_id: e.g. ``ABC1234DEF567-1R1234_ABC8DE0Q``.
       See ``ceph osd metadata | jq '.[].device_ids'``
    """
    __slots__ = ()


def _mk_orch_methods(cls):
    # Needs to be defined outside of for.
    # Otherwise meth is always bound to last key
    def shim(method_name):
        def inner(self, *args, **kwargs):
            completion = self._oremote(method_name, args, kwargs)
            return completion
        return inner

    for meth in Orchestrator.__dict__:
        if not meth.startswith('_') and meth not in ['is_orchestrator_module']:
            setattr(cls, meth, shim(meth))
    return cls


@_mk_orch_methods
class OrchestratorClientMixin(Orchestrator):
    """
    A module that inherents from `OrchestratorClientMixin` can directly call
    all :class:`Orchestrator` methods without manually calling remote.

    Every interface method from ``Orchestrator`` is converted into a stub method that internally
    calls :func:`OrchestratorClientMixin._oremote`

    >>> class MyModule(OrchestratorClientMixin):
    ...    def func(self):
    ...        completion = self.add_host('somehost')  # calls `_oremote()`
    ...        self._orchestrator_wait([completion])
    ...        self.log.debug(completion.result)

    """

    def set_mgr(self, mgr):
        # type: (MgrModule) -> None
        """
        Useable in the Dashbord that uses a global ``mgr``
        """

        self.__mgr = mgr  # Make sure we're not overwriting any other `mgr` properties

    def __get_mgr(self):
        try:
            return self.__mgr
        except AttributeError:
            return self

    def _oremote(self, meth, args, kwargs):
        """
        Helper for invoking `remote` on whichever orchestrator is enabled

        :raises RuntimeError: If the remote method failed.
        :raises OrchestratorError: orchestrator failed to perform
        :raises ImportError: no `orchestrator_cli` module or backend not found.
        """
        mgr = self.__get_mgr()

        try:
            o = mgr._select_orchestrator()
        except AttributeError:
            o = mgr.remote('orchestrator_cli', '_select_orchestrator')

        if o is None:
            raise NoOrchestrator()

        mgr.log.debug("_oremote {} -> {}.{}(*{}, **{})".format(mgr.module_name, o, meth, args, kwargs))
        return mgr.remote(o, meth, *args, **kwargs)


    def _orchestrator_wait(self, completions):
        # type: (List[Completion]) -> None
        """
        Wait for completions to complete (reads) or
        become persistent (writes).

        Waits for writes to be *persistent* but not *effective*.

        :param completions: List of Completions
        :raises NoOrchestrator:
        :raises RuntimeError: something went wrong while calling the process method.
        :raises ImportError: no `orchestrator_cli` module or backend not found.
        """
        while any(not c.has_result for c in completions):
            self.process(completions)
            self.__get_mgr().log.info("Operations pending: %s",
                                      sum(1 for c in completions if not c.has_result))
            if any(c.needs_result for c in completions):
                time.sleep(1)
            else:
                break


class OutdatableData(object):
    DATEFMT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, data=None, last_refresh=None):
        # type: (Optional[dict], Optional[datetime.datetime]) -> None
        self._data = data
        if data is not None and last_refresh is None:
            self.last_refresh = datetime.datetime.utcnow()
        else:
            self.last_refresh = last_refresh

    def json(self):
        if self.last_refresh is not None:
            timestr = self.last_refresh.strftime(self.DATEFMT)
        else:
            timestr = None

        return {
            "data": self._data,
            "last_refresh": timestr,
        }

    @property
    def data(self):
        return self._data

    # @data.setter
    # No setter, as it doesn't work as expected: It's not saved in store automatically

    @classmethod
    def time_from_string(cls, timestr):
        if timestr is None:
            return None
        # drop the 'Z' timezone indication, it's always UTC
        timestr = timestr.rstrip('Z')
        return datetime.datetime.strptime(timestr, cls.DATEFMT)


    @classmethod
    def from_json(cls, data):
        return cls(data['data'], cls.time_from_string(data['last_refresh']))

    def outdated(self, timeout=None):
        if timeout is None:
            timeout = 600
        if self.last_refresh is None:
            return True
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=timeout)
        return self.last_refresh < cutoff

    def __repr__(self):
        return 'OutdatableData(data={}, last_refresh={})'.format(self._data, self.last_refresh)


class OutdatableDictMixin(object):
    """
    Toolbox for implementing a cache. As every orchestrator has
    different needs, we cannot implement any logic here.
    """

    def __getitem__(self, item):
        # type: (str) -> OutdatableData
        return OutdatableData.from_json(super(OutdatableDictMixin, self).__getitem__(item))

    def __setitem__(self, key, value):
        # type: (str, OutdatableData) -> None
        val = None if value is None else value.json()
        super(OutdatableDictMixin, self).__setitem__(key, val)

    def items(self):
        # type: () -> Iterator[Tuple[str, OutdatableData]]
        for item in super(OutdatableDictMixin, self).items():
            k, v = item
            yield k, OutdatableData.from_json(v)

    def items_filtered(self, keys=None):
        if keys:
            return [(host, self[host]) for host in keys]
        else:
            return list(self.items())

    def any_outdated(self, timeout=None):
        items = self.items()
        if not list(items):
            return True
        return any([i[1].outdated(timeout) for i in items])

    def remove_outdated(self):
        outdated = [item[0] for item in self.items() if item[1].outdated()]
        for o in outdated:
            del self[o]

    def invalidate(self, key):
        self[key] = OutdatableData(self[key].data,
                                   datetime.datetime.fromtimestamp(0))


class OutdatablePersistentDict(OutdatableDictMixin, PersistentStoreDict):
    pass


class OutdatableDict(OutdatableDictMixin, dict):
    pass
