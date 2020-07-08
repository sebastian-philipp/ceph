"""
Automatically scale MDSs based on status of the file-system using the FSMap
"""

import logging
from typing import Optional, List
from mgr_module import MgrModule
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
import orchestrator
import copy

log = logging.getLogger(__name__)

class MDSAutoscaler(orchestrator.OrchestratorClientMixin, MgrModule):
    """
    MDS autoscaler.
    """
    def __init__(self, *args, **kwargs):
        super(MDSAutoscaler, self).__init__(*args, **kwargs)
        self.set_mgr(self)
        self.fs_map: Optional[dict] = None
        self.old_notify_type = "NONE"

    def get_service(self, fs_name) -> Optional[List[orchestrator.ServiceDescription]]:
        result = None
        try:
            service = "mds.{}".format(fs_name)
            completion = self.describe_service(service_type='mds',
                                               service_name=service,
                                               refresh=True)
            self._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
            result = completion.result
        except Exception as e:
            self.log.exception("{}: exception while fetching service info"
                               .format(e))
            pass
        self.log.info("service info:{}".format(result))
        return result

    def update_daemon_count(self, fs_name, abscount) -> Optional[ServiceSpec]:
        svclist = self.get_service(fs_name)
        if svclist is None or len(svclist) == 0:
            self.log.warn("Failed to fetch MDS service details for fs '{}'"
                          .format(fs_name))
            return None

        svc = svclist[0]
        self.log.info("newsvc:{}".format(svc))
        self.log.info("newsvc.spec:{}".format(svc.spec))
        self.log.info("new count:{}".format(abscount))

        # hosts = []
        # for h in svc.spec.placement.hosts:
        #     hosts.append(h.hostname)
        # ps = PlacementSpec(hosts=hosts, count=abscount)
        ps = copy.deepcopy(svc.spec.placement)
        newspec = ServiceSpec(service_type=svc.spec.service_type,
                             service_id=svc.spec.service_id,
                             placement=ps)
        ps.count = abscount
        return newspec

    def spawn_mds(self, fs_name, count):
        newsvc = self.update_daemon_count(fs_name, count+1)
        if newsvc is None:
            return

        completion = self.apply_mds(newsvc)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)

    def kill_mds(self, fs_name, count):
        newsvc = self.update_daemon_count(fs_name, count-1)
        if newsvc is None:
            return

        completion = self.apply_mds(newsvc)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)

    def get_required_standby_count(self, fs_name):
        total = 0
        assert self.fs_map is not None
        for fs in self.fs_map.get('filesystems'):
            if fs['mdsmap']['fs_name'] == fs_name:
                self.log.info("getting standby_count_wanted")
                return fs.get('mdsmap').get('standby_count_wanted')
        # total = max(total, fs.get('mdsmap').get('standby_count_wanted'))
        return total

    def get_current_standby_count(self, fs_name):
        # standbys are not grouped by filesystems in fs_map
        # available = standby_replay + standby_active
        assert self.fs_map is not None
        total = 0
        for sb in self.fs_map.get('standbys'):
            if orchestrator.service_name(sb.name) == 'mds.{}'.format(fs_name):
                total += 1
        # return len(self.fs_map.get('standbys'))
        return total

    def get_current_active_count(self, fs_name):
        assert self.fs_map is not None
        for fsys in self.fs_map.get('filesystems'):
            if fsys.get('mdsmap').get('fs_name') == fs_name:
                return len(fsys.get('mdsmap').get('in'))
        return 0

    def get_fs_name(self, index=0):
        assert self.fs_map is not None
        fs = self.fs_map.get('filesystems')[index]
        self.log.info("fs:{}".format(fs))
        fs_name = fs.get('mdsmap').get('fs_name')
        self.log.info("fs_name:{}".format(fs_name))
        return fs_name

    def verify_and_manage_mds_instance(self, fs_name):
        assert self.fs_map is not None
        standbys_required = self.get_required_standby_count(fs_name)
        standbys_current = self.get_current_standby_count(fs_name)
        active = self.get_current_active_count(fs_name)

        self.log.info("standbys_required:{0}, standbys_current:{1}"
                      .format(standbys_required, standbys_current))
        total = standbys_current + active
        if standbys_current > standbys_required:
            # remove one mds at a time and wait for updated fs_map
            self.log.info("killing standby mds ...")
            # self.kill_mds(fs_name, total)
            total -= 1
        elif standbys_current < standbys_required:
            # launch one mds at a time and wait for updated fs_map
            self.log.info("spawning standby mds ...")
            # self.spawn_mds(fs_name, total)
            total += 1

        newspec = self.update_daemon_count(fs_name, total)
        if newspec is None:
            self.log.info("new service spec for fs '{0}' is None!".format(fs_name))
            return

        self.log.info("new placement count:{0}".format(newspec.placement.count))

        completion = self.apply_mds(newspec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)

    def notify(self, notify_type, notify_id):
        # we don't know for which fs config has been changed
        self.log.info("received notification for {0}".format(notify_type))
        # if notify_type == 'fs_map' and self.old_notify_type == 'service_map':
        if notify_type != 'fs_map':
            return
        self.log.info("getting fs_map")
        self.fs_map = self.get('fs_map')
        self.log.info("fs_map type:{}".format(type(self.fs_map)))
        if not self.fs_map:
            return
        for fs in self.fs_map.get('filesystems'):
            fs_name = fs.get('mdsmap').get('fs_name')
            self.log.info("processing fs: {}".format(fs_name))
            self.verify_and_manage_mds_instance(fs_name)
        self.old_notify_type = notify_type
