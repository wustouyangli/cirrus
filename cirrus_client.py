# coding=utf-8

import functools
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.transport.TTransport import TBufferedTransportFactory
from util.common_util import CommonUtil
from client.host_selector import HostSelector
from zookeeper.zk_subscriber import ZkSubscriber
from client.client import Client
from client.client_pool import ClientPool

DEFAULT_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()
DEFAULT_TRANSPORT_FACTORY = TBufferedTransportFactory()


class CirrusClient(object):

    def __init__(self, thrift_module, tag=None, pool_size=1,
                 req_timeout=5000, socket_connection_timeout=1000,
                 pool_acquire_client_timeout=1000, retry_count=3,
                 protocol_factory=DEFAULT_PROTOCOL_FACTORY,
                 transport_factory=DEFAULT_TRANSPORT_FACTORY,
                 use_weight_host_selector=True):
        service_key = CommonUtil.get_service_key(thrift_module)
        zk_subscriber = ZkSubscriber(service_key, tag)
        host_selector = HostSelector(zk_subscriber, use_weight_host_selector)

        self._service_key = service_key
        self._host_selector = host_selector

        self._client_class_name = service_key + 'Client(%s)' % tag
        self._client_class = type(self._client_class_name, (thrift_module.Client, Client), {})

        self._pool_size = pool_size
        self._req_timeout = req_timeout
        self._socket_connection_timeout = socket_connection_timeout
        self._pool_acquire_client_timeout = pool_acquire_client_timeout
        self._retry_count = retry_count

        self._protocol_factory = protocol_factory
        self._transport_factory = transport_factory
        self._iface = thrift_module.Iface

        def close_client(client):
            client.disconnect()

        self._client_pool = ClientPool(pool_name=self._client_class_name + 'Pool',
                                       pool_size=pool_size,
                                       client_class=self._client_class,
                                       close_client_handler=close_client,
                                       host_selector=self._host_selector,
                                       req_timeout=self._req_timeout,
                                       socket_connection_timeout=self._socket_connection_timeout,
                                       retry_count=self._retry_count,
                                       protocol_factory=self._protocol_factory,
                                       transport_factory=self._transport_factory)

    def __getattr__(self, method_name):

        if not getattr(self._iface, method_name, None):
            return method_name

        @functools.wraps(method_name)
        def wrapper(*args, **kwargs):
            with self._client_pool.get_client(
                    block=True,
                    pool_acquire_client_timeout=self._pool_acquire_client_timeout,
                    req_timeout=self._req_timeout) as client:
                return getattr(client, method_name)(*args, **kwargs)

        return wrapper
