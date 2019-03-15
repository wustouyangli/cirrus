# coding=utf-8

import env_base
import gevent
from gevent import monkey

monkey.patch_all()
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
from client.client_pool import ClientPool
from util.schedule_task import ScheduleTask
from client.client import Client
from cirrus_client import CirrusClient
from test.oyl_thrift.gen_py.com.oyl import OylWorkService


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


class GC(object):
    def __init__(self):
        self._gc = ScheduleTask(
            name='gc-job',
            start_after_seconds=1,
            interval_seconds=2,
            handler=self._f
        )
        self._gc.run()

    def _f(self):
        print 'hi oyl'

    def __del__(self):
        self._gc.stop()


def test_schedule_task():
    gc = GC()
    print 'init gc'
    time.sleep(6)
    del gc


class MyClient(object):

    def __init__(self):
        print 'my client init'

    def say_hello(self):
        print 'hello'


def close_client(client):
    print 'close client'


def test_client_pool():
    client_pool = ClientPool(
        pool_name='test-client-pool',
        pool_size=5,
        client_class=MyClient,
        close_client_handler=close_client,

    )

    # with client_pool.get_client() as client:
    #     # time.sleep(6)  验证请求超时
    #     client.say_hello()

    def multi_acquire_client(client_pool):
        with client_pool.get_client() as client:
            # time.sleep(6)  验证请求超时
            time.sleep(2)
            client.say_hello()
    jobs = []
    for i in range(10):
        jobs.append(gevent.spawn(multi_acquire_client, client_pool))

    gevent.joinall(jobs)


def test_client():
    thrift_module = OylWorkService
    service_key = CommonUtil.get_service_key(thrift_module)
    tag = None
    client_class_name = service_key + 'Client(%s)' % tag
    # print service_key, tag, client_class_name
    client_class = type(client_class_name, (thrift_module.Client, Client), {})
    host_provider = HostProvider(service_key)
    host_selector = HostSelector(host_provider)
    client = client_class(host_selector=host_selector)
    op = 'sub'
    a = 10
    b = 2
    res = client.work(op, a, b)
    print res.result


def test_cirrus_client():
    thrift_module = OylWorkService

    client = CirrusClient(thrift_module)
    op = 'sub'
    a = 10
    b = 2
    res = client.work(op, a, b)
    print res.result


if __name__ == "__main__":
    # test_zk_client()
    # test_zk_publisher()
    # test_host_provider()
    # test_host_selector()
    # test_schedule_task()
    # test_client_pool()
    # test_client()
    test_cirrus_client()
