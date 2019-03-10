# coding=utf-8

import inspect
import functools
import time
import logging
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.transport.TTransport import TBufferedTransportFactory
from thrift.transport.TSocket import TSocket

logger = logging.getLogger(__name__)
DEFAULT_THRIFT_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()
DEFAULT_THRIFT_TRANSPORT_FACTORY = TBufferedTransportFactory()
SECS_FOR_DISCONNECT = 10


class EnsureConnectionClient(type):

    def __new__(mcs, class_name, class_bases, class_dict):
        thrift_client_class = class_bases[0]  # thrift client

        def __init__(self, host_selector, timeout=5000, socket_connection_timeout=1000,
                     retry_count=3, protocol_factory=DEFAULT_THRIFT_PROTOCOL_FACTORY,
                     transport_factory=DEFAULT_THRIFT_TRANSPORT_FACTORY):

            self._host_selector = host_selector
            # 选择host
            host = self._host_selector.get_host()
            ip, port = host.split(':')
            self._ip = ip
            self._port = int(port)
            self._timeout = timeout
            self._socket_connection_timeout = socket_connection_timeout
            self._retry_count = retry_count
            self._protocol_factory = protocol_factory
            self._transport_factory = transport_factory
            # 连接标志
            self._connected = False
            # 连接时间
            self._connected_at = 0

            self._socket = None
            self._transport = None
            self._protocol = None
            self._client = None

        class_dict['__init__'] = __init__

        def ensure_connection(method, service_name, method_name):
            @functools.wraps(method)
            def wrapper(self, *args, **kwargs):
                retry_count = self._retry_count
                left_try_count = retry_count
                while left_try_count:
                    start_time = time.time()
                    select_new = left_try_count == 1
                    try:
                        # 连接服务端
                        client = self.connect(select_new)
                        res = method(client, *args, **kwargs)
                        time_taken = time.time() - start_time
                        logger.info('Request: %s.%s (connect to %s:%s) call succeed, taken %s seconds.',
                                    service_name, method_name, self._host, self._port, time_taken)
                        # 更新过期连接
                        self.refresh_connection()
                        return res
                    except Exception as e:
                        time_taken = time.time() - start_time
                        logger.info('Request: %s.%s (connect to %s:%s) call failed, taken %s seconds, exception: %s',
                                    service_name, method_name, self._host, self._port, time_taken, e)
                        left_try_count -= 1
                        # 断开连接
                        self.disconnect()
                        if select_new:
                            self._host_selector.invalid_host()
                        if not left_try_count:
                            logger.info('Request: %s.%s call failed after all %s retries',
                                        service_name, method_name, retry_count)
                            raise

            return wrapper

        service_name = thrift_client_class.__module__.rapartition('.')[-1]
        for method_name, method in inspect.getmembers(thrift_client_class, predicate=inspect.ismethod):
            if method_name.startswith('__'):
                continue
            # 忽略非thrift_client_class类的方法
            attr = getattr(thrift_client_class, method_name, None)
            if attr is None or not inspect.ismethod(attr):
                continue

            class_dict[method_name] = ensure_connection(method, service_name, method_name)

        return type.__new__(mcs, class_name, class_bases, class_dict)


class Client(object):

    __metaclass__ = EnsureConnectionClient

    def connect(self, select_new=False):
        # 已连接,设置请求超时时间
        if self._connected:
            self._socket.setTimeout(self._timeout)
            return self._client

        if select_new:
            host = self._host_selector.get_host()
            ip, port = host.split(':')
            self._ip = ip
            self._port = int(port)

        self._socket = TSocket(self._ip, self._port)
        # 设置socket连接超时时间
        self._socket.setTimeout(self._socket_connection_timeout)
        self._transport = self._transport_factory.getTransport(self._socket)
        self._protocol = self._protocol_factory.getProtocol(self._transport)
        thrift_client_class = self.__class__.__bases__[0]

        self._client = thrift_client_class(self._protocol)
        self._connected = True
        self._connected_at = time.time()
        # 设置请求超时时间
        self._socket.setTimeout(self._timeout)

        return self._client

    def disconnect(self):
        if self._connected:
            self._transport.close()

        self._connected = False
        self._connected_at = 0
        self._socket = None
        self._transport = None
        self._protocol = None
        self._client = None

    def refresh_connection(self):
        if self._connected:
            if time.time() - self._connected_at > SECS_FOR_DISCONNECT:
                self.disconnect()
