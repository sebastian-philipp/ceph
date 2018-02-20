from __future__ import absolute_import

import cherrypy
import json
import re
from functools import partial

#from dashboard_v2.controllers.rbd import Rbd
from dashboard_v2.controllers.dashboard import Dashboard
from ..tools import RESTController, ViewCache, ApiController, AuthRequired, list_route
from .. import logger
import rbd


@ApiController('rbdmirroring')
@AuthRequired()
class RbdMirroring(RESTController):
    def __init__(self):
        self.pool_data = {}

    @ViewCache()
    def _daemons_and_pools(self):
        def get_daemons():
            daemons = []
            for server in self.mgr.list_servers():
                for service in server['services']:
                    if service['type'] == 'rbd-mirror':
                        id = service['id']
                        metadata = self.mgr.get_metadata('rbd-mirror', id)
                        status = self.mgr.get_daemon_status('rbd-mirror', id)
                        try:
                            status = json.loads(status['json'])
                        except:
                            status = {}

                        instance_id = metadata['instance_id']
                        if id == instance_id:
                            # new version that supports per-cluster leader elections
                            id = metadata['id']

                        # extract per-daemon service data and health
                        daemon = {
                            'id': id,
                            'instance_id': instance_id,
                            'version': metadata['ceph_version'],
                            'server_hostname': server['hostname'],
                            'service': service,
                            'server': server,
                            'metadata': metadata,
                            'status': status
                        }
                        daemon = dict(daemon, **get_daemon_health(daemon))
                        daemons.append(daemon)

            return sorted(daemons, key=lambda k: k['instance_id'])

        def get_daemon_health(daemon):
            health = {
                'health_color': 'info',
                'health': 'Unknown'
            }
            for pool_id, pool_data in daemon['status'].items():
                if (health['health'] != 'error' and
                   [k for k, v in pool_data.get('callouts', {}).items() if v['level'] == 'error']):
                    health = {
                        'health_color': 'error',
                        'health': 'Error'
                    }
                elif (health['health'] != 'error' and
                      [k for k, v in pool_data.get('callouts', {}).items() if v['level'] == 'warning']):
                    health = {
                        'health_color': 'warning',
                        'health': 'Warning'
                    }
                elif health['health_color'] == 'info':
                    health = {
                        'health_color': 'success',
                        'health': 'OK'
                    }
            return health

        def get_pools(daemons):
            Dashboard.mgr = self.mgr
            dashboard = Dashboard()
            status, pool_names = dashboard._rbd_pool_ls()  # FIXME: move out of dashboard
            if pool_names is None:
                logger.warning("Failed to get RBD pool list")
                return {}

            pool_stats = {}
            rbdctx = rbd.RBD()
            for pool_name in pool_names:
                logger.debug("Constructing IOCtx " + pool_name)
                try:
                    ioctx = self.mgr.rados.open_ioctx(pool_name)
                except:
                    logger.exception("Failed to open pool " + pool_name)
                    continue

                try:
                    mirror_mode = rbdctx.mirror_mode_get(ioctx)
                except:
                    logger.exception("Failed to query mirror mode " + pool_name)
                    raise

                stats = {}
                if mirror_mode == rbd.RBD_MIRROR_MODE_DISABLED:
                    continue
                elif mirror_mode == rbd.RBD_MIRROR_MODE_IMAGE:
                    mirror_mode = "image"
                elif mirror_mode == rbd.RBD_MIRROR_MODE_POOL:
                    mirror_mode = "pool"
                else:
                    mirror_mode = "unknown"
                    stats['health_color'] = "warning"
                    stats['health'] = "Warning"

                pool_stats[pool_name] = dict(stats, **{
                    'mirror_mode': mirror_mode
                })

            for daemon in daemons:
                for pool_id, pool_data in daemon['status'].items():
                    stats = pool_stats.get(pool_data['name'], None)
                    if stats is None:
                        continue

                    if pool_data.get('leader', False):
                        # leader instance stores image counts
                        stats['leader_id'] = daemon['metadata']['instance_id']
                        stats['image_local_count'] = pool_data.get('image_local_count', 0)
                        stats['image_remote_count'] = pool_data.get('image_remote_count', 0)

                    if (stats.get('health_color', '') != 'error' and
                        pool_data.get('image_error_count', 0) > 0):
                        stats['health_color'] = 'error'
                        stats['health'] = 'Error'
                    elif (stats.get('health_color', '') != 'error' and
                          pool_data.get('image_warning_count', 0) > 0):
                        stats['health_color'] = 'warning'
                        stats['health'] = 'Warning'
                    elif stats.get('health', None) is None:
                        stats['health_color'] = 'success'
                        stats['health'] = 'OK'

            for name, stats in pool_stats.items():
                if stats.get('health', None) is None:
                    # daemon doesn't know about pool
                    stats['health_color'] = 'error'
                    stats['health'] = 'Error'
                elif stats.get('leader_id', None) is None:
                    # no daemons are managing the pool as leader instance
                    stats['health_color'] = 'warning'
                    stats['health'] = 'Warning'
            return pool_stats
        
        daemons = get_daemons()
        return {
            'daemons': daemons,
            'pools': get_pools(daemons)
        }
    
    @ViewCache()
    def _pool_datum(self, pool_name):
        data = {}
        logger.debug("Constructing IOCtx " + pool_name)
        try:
            ioctx = self.mgr.rados.open_ioctx(pool_name)
        except:
            logger.exception("Failed to open pool " + pool_name)
            return None

        mirror_state = {
            'down': {
                'health': 'issue',
                'state_color': 'warning',
                'state': 'Unknown',
                'description': None
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_UNKNOWN: {
                'health': 'issue',
                'state_color': 'warning',
                'state': 'Unknown'
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_ERROR: {
                'health': 'issue',
                'state_color': 'error',
                'state': 'Error'
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_SYNCING: {
                'health': 'syncing'
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY: {
                'health': 'ok',
                'state_color': 'success',
                'state': 'Starting'
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_REPLAYING: {
                'health': 'ok',
                'state_color': 'success',
                'state': 'Replaying'
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY: {
                'health': 'ok',
                'state_color': 'success',
                'state': 'Stopping'
            },
            rbd.MIRROR_IMAGE_STATUS_STATE_STOPPED: {
                'health': 'ok',
                'state_color': 'info',
                'state': 'Primary'
            }
        }

        rbdctx = rbd.RBD()
        try:
            mirror_image_status = rbdctx.mirror_image_status_list(ioctx)
            data['mirror_images'] = sorted([
                dict({
                    'name': image['name'],
                    'description': image['description']
                }, **mirror_state['down' if not image['up'] else image['state']])
                for image in mirror_image_status
            ], key=lambda k: k['name'])
        except rbd.ImageNotFound:
            pass
        except:
            logger.exception("Failed to list mirror image status " + pool_name)

        return data

    @list_route(methods=['GET'])
    def content_data(self):
        status, data = self._content_data()
        return data

    @ViewCache()
    def _content_data(self):
        def get_pool_datum(pool_name):
            pool_datum = self.pool_data.get(pool_name, None)
            if pool_datum is None:
                self.pool_data[pool_name] = partial(self._pool_datum, pool_name)

            status, value = pool_datum()
            return value

        Dashboard.mgr = self.mgr
        dashboard = Dashboard()
        status, pool_names = dashboard._rbd_pool_ls()  # FIXME: move out of dashboard
        if pool_names is None:
            logger.warning("Failed to get RBD pool list")
            return None

        status, data = self._daemons_and_pools()
        if data is None:
            logger.warning("Failed to get rbd-mirror daemons list")
            data = {}
        daemons = data.get('daemons', [])
        pool_stats = data.get('pools', {})

        pools = []
        image_error = []
        image_syncing = []
        image_ready = []
        for pool_name in pool_names:
            pool = get_pool_datum(pool_name) or {}
            stats = pool_stats.get(pool_name, {})
            if stats.get('mirror_mode', None) is None:
                continue

            mirror_images = pool.get('mirror_images', [])
            for mirror_image in mirror_images:
                image = {
                    'pool_name': pool_name,
                    'name': mirror_image['name']
                }

                if mirror_image['health'] == 'ok':
                    image.update({
                        'state_color': mirror_image['state_color'],
                        'state': mirror_image['state'],
                        'description': mirror_image['description']
                    })
                    image_ready.append(image)
                elif mirror_image['health'] == 'syncing':
                    p = re.compile("bootstrapping, IMAGE_COPY/COPY_OBJECT (.*)%")
                    image.update({
                        'progress': (p.findall(mirror_image['description']) or [0])[0]
                    })
                    image_syncing.append(image)
                else:
                    image.update({
                        'state_color': mirror_image['state_color'],
                        'state': mirror_image['state'],
                        'description': mirror_image['description']
                    })
                    image_error.append(image)

            pools.append(dict({
                'name': pool_name
            }, **stats))

        return {
            'daemons': daemons,
            'pools' : pools,
            'image_error': image_error,
            'image_syncing': image_syncing,
            'image_ready': image_ready
        }
        
    def list(self):
        status, data = self._daemons_and_pools()
        if isinstance(data, Exception):
            logger.exception("Failed to get rbd-mirror daemons and pools")
            raise type(data)(str(data))
        else:
            daemons = data.get('daemons', [])
            pools = data.get('pools', {})

        warnings = 0
        errors = 0
        for daemon in daemons:
            if daemon['health_color'] == 'error':
                errors += 1
            elif daemon['health_color'] == 'warning':
                warnings += 1
        for pool_name, pool in pools.items():
            if pool['health_color'] == 'error':
                errors += 1
            elif pool['health_color'] == 'warning':
                warnings += 1
        return {'warnings': warnings, 'errors': errors}
