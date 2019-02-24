# coding=utf-8

import os
import socket

env_dict = os.environ


class EnvType(object):
    DEV = 'dev'
    TEST = 'test'
    PROD = 'prod'


class CommonUtil(object):
    SERVICE_TAG_KEY = 'SERVICE_TAG'
    SERVICE_WEIGHT_KEY = 'SERVICE_WEIGHT'
    ENV_KEY = 'CIRRUS_ENV'
    _HOSTNAME = None

    @classmethod
    def get_service_key(cls, thrift_module):
        name = thrift_module.__name__
        # 将模块名分成两部分,以最后一个.作为分隔符,取后半部分
        key = name.rapartition('.')[-1]
        return key

    @classmethod
    def get_random_port(cls):
        soc = socket.socket()
        soc.bind(('', 0))
        port = (soc.getsockname()[1])
        soc.close()
        return port

    @classmethod
    def get_service_tag(cls, tag=None):
        if tag is not None:
            return tag
        # 环境变量设置tag
        return env_dict.get(cls.SERVICE_TAG_KEY, None)

    @classmethod
    def get_service_weight(cls, weight=None):
        if weight is not None:
            return weight
        return env_dict.get(cls.SERVICE_WEIGHT_KEY, 0)

    @classmethod
    def is_prod(cls):
        return env_dict.get(cls.ENV_KEY, EnvType.DEV) == EnvType.PROD

    @classmethod
    def get_hostname(cls):
        if cls._HOSTNAME is None:
            cls._HOSTNAME = socket.gethostname()
        return cls._HOSTNAME

    @classmethod
    def get_exception_errno(cls, e):
        if hasattr(e, 'errno'):
            return e.errno
        elif e.args:
            return e.args[0]
        return None
