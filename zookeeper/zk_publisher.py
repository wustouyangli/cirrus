# coding=utf-8

import json
import atexit
from zookeeper.zk_client import ZkClient
from kazoo.protocol.states import KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from server.instance_config_data import InstanceConfigData
import logging

logger = logging.getLogger(__name__)


class ZkPublisher(ZkClient):

    def __init__(self, service_key, instance_path):
        super(ZkPublisher, self).__init__()
        self._service_key = service_key
        self._instance_path = instance_path
        self._service_path = '%s/%s' % (self._zk_path, self._service_key)
        self._node_path = '%s/%s' % (self._service_path, self._instance_path)
        self._instance_config_data = None

    def register(self, instance_config_data):
        assert isinstance(instance_config_data, InstanceConfigData)
        self._instance_config_data = instance_config_data
        self._client.add_listener(self._state_listener)
        try:
            self._ensure_path()
            self._client.create(self._node_path, self._serialize_instance_config_data(), ephemeral=True)
        except NodeExistsError:
            logger.error('Zookeeper node exists, path: %s', self._node_path)
        atexit.register(self.stop)

    def modify(self, instance_config_data):
        assert isinstance(instance_config_data, InstanceConfigData)
        self._instance_config_data = instance_config_data
        try:
            self._ensure_path()
            self._client.set(self._node_path, self._serialize_instance_config_data())
        except NoNodeError:
            logger.error('Zookeeper node does not exists, path: %s', self._node_path)

    def _state_listener(self, state):
        if state in [KazooState.LOST, KazooState.SUSPENDED]:
            self._connection_lost = True
            logger.info('Zookeeper connection lost, current state: %s', state)
        elif state == KazooState.CONNECTED and self._connection_lost:
            # 重新连接
            self._client.handler.spawn(self.register, self._instance_config_data)
            self._connection_lost = False
            logger.info('Zookeeper reconnection, current state: %s', state)

    def _ensure_path(self):
        self._client.ensure_path(self._service_path)

    def _serialize_instance_config_data(self):
        if not isinstance(self._instance_config_data, InstanceConfigData):
            raise Exception('Zookeeper register invalid service config data')
        return json.dumps(self._instance_config_data.to_dict())


