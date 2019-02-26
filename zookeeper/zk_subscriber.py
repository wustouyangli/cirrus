# coding=utf-8

import os
import glob
import json
import random
import logging
from zookeeper.zk_client import ZkClient
from server.service_config_data import ServiceConfigData

logger = logging.getLogger(__name__)


class ZkSubscriber(ZkClient):

    def __init__(self, service_key):
        super(ZkSubscriber, self).__init__()
        self._service_key = service_key
        self._service_path = '%s/%s' % (self._zk_path, self._service_key)
        self._hosts = {}

    def get_instances(self):
        # 通配符匹配路径下所有文件或目录
        regex = os.path.join(self._service_path, '*')
        files = glob.glob(regex)
        random.shuffle(files)
        for file in files:
            if os.path.isfile(file):
                host_info = file.split(':')
                ip = host_info[0]
                port = host_info[1]
                host = '%s:%s' % (ip, port)

                need_update = False
                # 文件修改时间
                mtime = os.stat(file).st_mtime
                if host in self._hosts:
                    if mtime > self._hosts[host].get('mtime'):
                        need_update = True
                else:
                    need_update = True

                if need_update:
                    with open(file, 'r') as f:
                        info = f.read()
                    try:
                        info = json.loads(info)
                    except Exception as e:
                        info = {}
                        logger.info('json loads service config data error, host: %', host)
                    info = ServiceConfigData(**info)
                    self._hosts[host] = dict(info=info, mtime=mtime)

        return self._hosts


