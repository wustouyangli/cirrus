# coding=utf-8

import os
import socket

env_dict = os.environ


class ZkUtil(object):
    ZK_HOSTS_KEY = 'ZK_HOSTS_KEY'
    ZK_PATH_KEY = 'ZK_PATH_KEY'
    DEFAULT_ZK_PATH = '/cirrus/zookeeper'

    @classmethod
    def get_zk_hosts(cls):
        hosts = env_dict.get(cls.ZK_HOSTS_KEY, None)
        if not hosts:
            ip = socket.gethostbyname(socket.gethostname())
            port = '2181'
            hosts = ip + ':' + port
        return hosts

    @classmethod
    def get_zk_path(cls):
        return env_dict.get(cls.ZK_PATH_KEY, cls.DEFAULT_ZK_PATH)
