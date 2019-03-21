# coding=utf-8

import os
import sys
import time
import psutil
import logging
import signal
import atexit
from util.common_util import CommonUtil
from zookeeper.zk_client import ZkClient
from util.schedule_task import ScheduleTask

logger = logging.getLogger(__name__)

ZK_WATCHER_PID_FILE = 'watcher.pid'
ZK_WATCHER_LOG_FILE = 'watcher.log'
DEFAULT_LOG_FORMAT = '%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s'


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
        print 'hi, oyl'

    def stop(self):
        super(ZkWatcher, self).stop()
        if self._watcher:
            self._watcher.stop()

    def _sig_exit(self, signum, frame):
        logger.error('Zookeeper watcher(process id: %d) receive signal %d', os.getpid(), signum)
        self.stop()
        sys.exit(0)
