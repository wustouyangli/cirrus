# coding=utf-8

import env_base
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s')

import json
import time
from zookeeper.zk_client import ZkClient
from zookeeper.zk_publisher import ZkPublisher

from server.instance_config_data import InstanceConfigData
from util.common_util import CommonUtil
from client.host_provider import HostProvider
from client.host_selector import HostSelector


def test_zk_client():
    zk_client = ZkClient()
    zk_client.stop()


def test_zk_publisher():
    # zk_publisher = ZkPublisher(service_key='cirrus', instance_path='127.0.1.1:12340')
    # instance_config_data = InstanceConfigData(tag='oyl', weight=100, hostname=CommonUtil.get_hostname())
    # zk_publisher.register(instance_config_data)
    instance_config_data = InstanceConfigData(tag=None, weight=50, hostname=CommonUtil.get_hostname())
    print json.dumps(instance_config_data.to_dict())
    # zk_publisher.modify(instance_config_data)


def test_host_provider():
    host_provider = HostProvider(service_key='cirrus')
    instances = host_provider.get_instances()
    for instance in instances:
        print instance.to_dict()


def test_host_selector():
    host_provider = HostProvider(service_key='cirrus')
    host_selector = HostSelector(host_provider, expire_time=2, retry_time=2)
    # host = host_selector.get_host()
    # print host
    # host_selector.invalid_host()
    # host = host_selector.get_host()
    # print host

    for i in range(8):
        host = host_selector.get_host()
        print host
        time.sleep(1)
        if i == 4:
            host_selector.invalid_host()


if __name__ == "__main__":
    # test_zk_client()
    # test_zk_publisher()
    # test_host_provider()
    test_host_selector()
