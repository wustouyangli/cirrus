# coding=utf-8

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from util.common_util import CommonUtil
from client.host_selector import HostSelector
from zookeeper.zk_subscriber import ZkSubscriber

'''
1.从连接池里获取一个连接
    1.连接选取一个host(ip:port)进行连接
    2.连接成功则返回连接
    3.连接失败则抛异常
2.获取到连接则传输数据
    1.在限定时间内收到响应
    2.超时未收到响应则报错
3.获取连接失败则继续尝试,3次失败则报错
'''


class CirrusClient(object):
    DEFAULT_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()

    def __init__(self, thrift_module, hosts=[], tag=None, pool_size=1,
                 timeout=1000, protocol_factory=DEFAULT_PROTOCOL_FACTORY,
                 use_weight_host_selector=True):
        service_key = CommonUtil.get_service_key(thrift_module)
        zk_subscriber = ZkSubscriber(service_key, tag)
        host_selector = HostSelector(zk_subscriber, use_weight_host_selector)

        self._service_key = service_key
        self._host_selector = host_selector

