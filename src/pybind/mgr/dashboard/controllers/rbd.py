# -*- coding: utf-8 -*-
from __future__ import absolute_import

import math
from contextlib import contextmanager
from functools import wraps

import rbd

from . import ApiController, AuthRequired, RESTController
from .. import mgr
from ..tools import ViewCache
from ..services.exception import c2d, handle_rados_error, handle_rbd_error


@ApiController('rbd')
@AuthRequired()
class Rbd(RESTController):

    RBD_FEATURES_NAME_MAPPING = {
        rbd.RBD_FEATURE_LAYERING: "layering",
        rbd.RBD_FEATURE_STRIPINGV2: "striping",
        rbd.RBD_FEATURE_EXCLUSIVE_LOCK: "exclusive-lock",
        rbd.RBD_FEATURE_OBJECT_MAP: "object-map",
        rbd.RBD_FEATURE_FAST_DIFF: "fast-diff",
        rbd.RBD_FEATURE_DEEP_FLATTEN: "deep-flatten",
        rbd.RBD_FEATURE_JOURNALING: "journaling",
        rbd.RBD_FEATURE_DATA_POOL: "data-pool",
        rbd.RBD_FEATURE_OPERATIONS: "operations",
    }

    def __init__(self):
        super(Rbd, self).__init__()
        self.rbd = None

    @staticmethod
    def _format_bitmask(features):
        """
        Formats the bitmask:

        >>> Rbd._format_bitmask(45)
        ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']
        """
        names = [val for key, val in Rbd.RBD_FEATURES_NAME_MAPPING.items()
                 if key & features == key]
        return sorted(names)

    @staticmethod
    def _format_features(features):
        """
        Converts the features list to bitmask:

        >>> Rbd._format_features(['deep-flatten', 'exclusive-lock', 'layering', 'object-map'])
        45

        >>> Rbd._format_features(None) is None
        True

        >>> Rbd._format_features('not a list') is None
        True
        """
        if not features or not isinstance(features, list):
            return None

        res = 0
        for key, value in Rbd.RBD_FEATURES_NAME_MAPPING.items():
            if value in features:
                res = key | res
        return res

    @ViewCache()
    def _rbd_list(self, pool_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        self.rbd = rbd.RBD()
        names = self.rbd.list(ioctx)
        result = []
        for name in names:
            i = rbd.Image(ioctx, name)
            stat = i.stat()
            stat['name'] = name
            stat['id'] = i.id()
            features = i.features()
            stat['features'] = features
            stat['features_name'] = self._format_bitmask(features)

            # the following keys are deprecated
            del stat['parent_pool']
            del stat['parent_name']

            try:
                parent_info = i.parent_info()
                parent = "{}@{}".format(parent_info[0], parent_info[1])
                if parent_info[0] != pool_name:
                    parent = "{}/{}".format(parent_info[0], parent)
                stat['parent'] = parent
            except rbd.ImageNotFound:
                pass
            result.append(stat)
        return result

    @c2d(handle_rbd_error)
    @c2d(handle_rados_error, 'pool')
    def get(self, pool_name):
        # pylint: disable=unbalanced-tuple-unpacking
        status, value = self._rbd_list(pool_name)
        return {'status': status, 'value': value}

    @c2d(handle_rbd_error)
    @c2d(handle_rados_error, 'pool')
    @RESTController.args_from_json
    def create(self, name, pool_name, size, obj_size=None, features=None, stripe_unit=None,
               stripe_count=None, data_pool=None):
        if not self.rbd:
            self.rbd = rbd.RBD()

        obj_size, features, stripe_unit, stripe_count, data_pool = [x if x else None for x in [
            obj_size, features, stripe_unit, stripe_count, data_pool
        ]]

        # Set order
        order = None
        if obj_size and float(obj_size) > 0:
            order = int(round(math.log(float(obj_size), 2)))

        # Set features
        feature_bitmask = self._format_features(features)

        with mgr.rados.open_ioctx(pool_name) as ioctx:
            self.rbd.create(ioctx, name, int(size), order=order, old_format=False,
                            features=feature_bitmask, stripe_unit=stripe_unit,
                            stripe_count=stripe_count, data_pool=data_pool)
        return {'success': True}
