# coding=utf-8

import env_base
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s')

from zookeeper.zk_client import ZkClient
from zookeeper.zk_publisher import ZkPublisher

from server.instance_config_data import InstanceConfigData
from util.common_util import CommonUtil


def test_zk_client():
    zk_client = ZkClient()
    zk_client.stop()


def test_zk_publisher():
    zk_publisher = ZkPublisher(service_key='cirrus', instance_path='127.0.1.1:12340')
    # instance_config_data = InstanceConfigData(tag='oyl', weight=100, hostname=CommonUtil.get_hostname())
    # zk_publisher.register(instance_config_data)
    instance_config_data = InstanceConfigData(tag='oyl', weight=50, hostname=CommonUtil.get_hostname())
    zk_publisher.modify(instance_config_data)


if __name__ == "__main__":
    # test_zk_client()
    test_zk_publisher()
