# coding=utf-8

import os
import glob
import json
import random
import logging
from zookeeper.zk_client import ZkClient
from server.instance_config_data import InstanceConfigData, InstanceConfigDataExtension

logger = logging.getLogger(__name__)


class ZkSubscriber(ZkClient):

    def __init__(self, service_key, tag=None):
        super(ZkSubscriber, self).__init__()
        self._service_key = service_key
        self._service_path = '%s/%s' % (self._zk_path, self._service_key)
        self._hosts = {}
        self._tag = tag

    def get_instances(self):
        # 通配符匹配路径下所有文件或目录
        regex = os.path.join(self._service_path, '*')
        files = glob.glob(regex)
        random.shuffle(files)
        instances = []
        for file in files:
            if os.path.isfile(file):
                host_info = file.split(':')
                ip = host_info[0]
                port = host_info[1]
                host = '%s:%s' % (ip, port)

                need_read = False
                # 文件修改时间
                mtime = os.stat(file).st_mtime
                if host in self._hosts.keys():
                    instance_config_data = self._hosts[host].instance_config_data
                    # 客户端tag不为空时,检查实例tag信息是否与客户端tag相同
                    if self._tag is not None and instance_config_data.tag != self._tag:
                        continue

                    if mtime > self._hosts[host].mtime:
                        need_read = True
                else:
                    need_read = True

                if need_read:
                    with open(file, 'r') as f:
                        instance_config_data = f.read()
                    try:
                        instance_config_data = json.loads(instance_config_data)
                    except Exception as e:
                        instance_config_data = {}
                        logger.info('json loads service config data error, host: %', host)
                    instance_config_data = InstanceConfigData(**instance_config_data)
                    self._hosts[host] = InstanceConfigDataExtension(host=host, instance_config_data=instance_config_data, mtime=mtime)
                instances.append(self._hosts[host])
        return instances


