# coding=utf-8

import inspect
import functools
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.transport.TTransport import TFramedTransportFactory
from util.common_util import CommonUtil
from client.host_selector import HostSelector
from client.host_provider import HostProvider
from client.client import Client
from client.client_pool import ClientPool

DEFAULT_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()
MILLIS_PER_SEC = 1000.0


class CirrusClient(object):

    def __init__(self, thrift_module, tag=None, pool_size=1,
                 req_timeout=5000, socket_connection_timeout=1000,
                 pool_acquire_client_timeout=1000, retry_count=3,
                 protocol_factory=DEFAULT_PROTOCOL_FACTORY,
                 use_weight_host_selector=True):
        service_key = CommonUtil.get_service_key(thrift_module)
        host_provider = HostProvider(service_key, tag)
        host_selector = HostSelector(host_provider, use_weight_host_selector)

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
        self._transport_factory = TFramedTransportFactory()
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
        method = getattr(self._iface, method_name, None)
        if method is None or not inspect.ismethod(method):
            return None

        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            with self._client_pool.get_client(
                    block=True,
                    pool_acquire_client_timeout=self._pool_acquire_client_timeout/MILLIS_PER_SEC,
                    req_timeout=self._req_timeout/MILLIS_PER_SEC) as client:
                return getattr(client, method_name)(*args, **kwargs)

        return wrapper
