# coding=utf-8

import json
import atexit
from zookeeper.zk_client import ZkClient
from kazoo.protocol.states import KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from server.service_config_data import ServiceConfigData
import logging

logger = logging.getLogger(__name__)


class ZkPublisher(ZkClient):

    def __init__(self, instance_path):
        super(ZkPublisher, self).__init__()
        self._instance_path = instance_path
        self._node_path = '%s/%s' % (self._zk_path, self._instance_path)
        self._info = None

    def register(self, info):
        self._info = info
        self._client.add_listener(self._state_listener)
        try:
            self._ensure_path()
            self._client.create(self._node_path, self._serialize_info())
        except NodeExistsError:
            logger.info('zookeeper node exists, path: %s', self._node_path)
        atexit.register(self.stop)

    def modify(self, info):
        self._info = info
        try:
            self._ensure_path()
            self._client.set(self._node_path, self._serialize_info())
        except NoNodeError:
            logger.info('zookeeper node does not exists, path: %s', self._node_path)

    def _state_listener(self, state):
        if state in [KazooState.LOST, KazooState.SUSPENDED]:
            self._connection_lost = True
            logger.info('zookeeper connection lost, current state: %s', state)
        elif state == KazooState.CONNECTED and self._connection_lost:
            # 重新连接
            self._client.handler.spawn(self.register, self._info)
            self._connection_lost = False
            logger.info('zookeeper reconnection, current state: %s', state)

    def _serialize_info(self):
        if not isinstance(self._info, ServiceConfigData):
            raise Exception('zookeeper register invalid service config data')
        return json.dumps(self._info.to_dict())


