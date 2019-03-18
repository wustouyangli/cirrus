# coding=utf-8

from kazoo.client import KazooClient
from zookeeper.zk_util import ZkUtil
import logging

logger = logging.getLogger(__name__)


class ZkClient(object):

    def __init__(self):
        zk_path = ZkUtil.get_zk_path()
        if not zk_path.startswith('/'):
            raise Exception("Zookeeper path must start with '/'(absolute path)")
        self._zk_path = zk_path
        self._connection_lost = False
        self._stop_flag = True
        self._hosts = ZkUtil.get_zk_hosts()
        self._client = KazooClient(hosts=self._hosts)
        self.start()

    def start(self):
        if self._stop_flag:
            self._client.start()
            self._stop_flag = False
            logger.info('Zk client start, hosts: %s', self._hosts)

    def stop(self):
        if not self._stop_flag:
            self._client.stop()
            self._stop_flag = True
            logger.info('Zookeeper stop, hosts: %s', self._hosts)

