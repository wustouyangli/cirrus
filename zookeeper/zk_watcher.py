# coding=utf-8

import os
import sys
import time
import psutil
import logging
import signal
import atexit
import glob
from util.common_util import CommonUtil
from zookeeper.zk_client import ZkClient
from util.schedule_task import ScheduleTask
from kazoo.protocol.states import KazooState

logger = logging.getLogger(__name__)

ZK_WATCHER_PID_FILE = 'watcher.pid'
ZK_WATCHER_LOG_FILE = 'watcher.log'
DEFAULT_LOG_FORMAT = '%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s'


class InstanceNode(object):
    def __init__(self, service_path, instance_host):
        self._service_path = service_path
        self._instance_host = instance_host
        if not os.path.exists(self._service_path):
            os.makedirs(self._service_path)
        self._instance_path = os.path.join(self._service_path, self._instance_host)
        self._data = None

    def update(self, data):
        if data is None:
            if os.path.exists(self._instance_path):
                logger.info('Remove instance path: %s', self._instance_path)
                os.remove(self._instance_path)
        else:
            if self._data is None or self._data != data:
                file_bak = self._instance_path + '.bak'
                with open(file_bak, 'w') as f:
                    f.write(data)
                os.rename(file_bak, self._instance_path)
                self._data = data
                logger.info('Update instance path: %s, data: %s', self._instance_path, data)


class ZkWatcher(ZkClient):

    def __init__(self, daemon=False, log_level=logging.INFO, log_format=None):
        super(ZkWatcher, self).__init__()
        if not os.path.exists(self._zk_path):
            os.makedirs(self._zk_path)
        log_format = log_format or DEFAULT_LOG_FORMAT
        if daemon:
            log_file = os.path.join(self._zk_path, ZK_WATCHER_LOG_FILE)
            logging.basicConfig(filename=log_file, format=log_format, level=log_level)
        else:
            logging.basicConfig(format=log_format, level=log_level)
        self._pid_file = os.path.join(self._zk_path, ZK_WATCHER_PID_FILE)
        self._watcher = None
        self._instance_node_map = {}

    def watch(self, start_after_seconds=0, interval_seconds=1):
        if os.path.exists(self._pid_file):
            with open(self._pid_file, 'r') as f:
                pid = f.read()

            if not pid or not psutil.pid_exists(int(pid)):
                logger.info('Process %s does not exists, remove pid file', pid)
                os.remove(self._pid_file)
            else:
                logger.info('Process %s already exists...', pid)
                sys.exit(1)
        signal.signal(signal.SIGTERM, self._sig_exit)
        signal.signal(signal.SIGINT, self._sig_exit)
        CommonUtil.set_proctitle('zk-watcher')
        with open(self._pid_file, 'w') as f:
            f.write(str(os.getpid()))

        atexit.register(self.stop)
        self._watcher = ScheduleTask('ZkWatcher', start_after_seconds, interval_seconds, self._start)
        self._watcher.run()

        while not self._stop_flag:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.stop()

    def _start(self):
        self._client.add_listener(self._state_listener)
        if self._connection_lost:
            return
        service_keys = self._client.get_children(self._zk_path)
        for service_key in service_keys:

            if self._connection_lost:
                return
            service_path = os.path.join(self._zk_path, service_key)
            instance_hosts = self._client.get_children(service_path)
            for instance_host in instance_hosts:
                instance_path = os.path.join(service_path, instance_host)
                if self._connection_lost:
                    return
                data, _ = self._client.get(instance_path)
                if instance_path not in self._instance_node_map:
                    self._instance_node_map[instance_path] = InstanceNode(service_path, instance_host)
                self._instance_node_map[instance_path].update(data)

        regex = os.path.join(self._zk_path, '*')
        service_dirs = glob.glob(regex)
        for service_dir in service_dirs:
            if not os.path.isdir(service_dir):
                continue
            service_abspath = os.path.abspath(service_dir)
            regex = os.path.join(service_abspath, '*')
            instance_files = glob.glob(regex)
            if not bool(instance_files):
                os.rmdir(service_dir)
            for instance_file in instance_files:
                if not os.path.isfile(instance_file):
                    continue
                host = os.path.basename(instance_file)
                host_info = host.split(':')
                if len(host_info) == 2 and host_info[1].isdigit():
                    instance_abspath = os.path.abspath(instance_file)

                    if self._connection_lost:
                        return
                    if not self._client.exists(instance_abspath):
                        os.remove(instance_abspath)
                        logger.info('Remove instance path: %s', instance_abspath)

    def _state_listener(self, state):
        if state in [KazooState.LOST, KazooState.SUSPENDED]:
            self._connection_lost = True
            logger.info('(Zk-watcher)Zookeeper connection lost, current state: %s', state)
        elif state == KazooState.CONNECTED and self._connection_lost:
            # 重新连接
            self._connection_lost = False
            logger.info('(Zk-watcher)Zookeeper reconnection, current state: %s', state)

    def stop(self):
        super(ZkWatcher, self).stop()
        if self._watcher:
            self._watcher.stop()

    def _sig_exit(self, signum, frame):
        logger.error('Zookeeper watcher(process id: %d) receive signal %d', os.getpid(), signum)
        self.stop()
        sys.exit(0)
