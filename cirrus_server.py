# coding=utf-8

import os
import inspect
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from util.common_util import CommonUtil
from server.service_config_data import ServiceConfigData
from transport.thrift_server_socket import ThriftServerSocket


class CirrusServer(object):

    DEFAULT_THRIFT_PROTOCOL_FACTORY = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()
    DEFAULT_THRIFT_LISTEN_QUEUE_SIZE = 128
    DEFAULT_WORKER_PROCESS_NUMBER = 4
    DEFAULT_HARAKIRI = 5
    DEFAULT_EVENT_QUEUE_SIZE = 100
    DEFAULT_THRIFT_RECV_TIMEOUT = 50000

    def __init__(self, thrift_module, handler, port=0,
                 protocol_factory=DEFAULT_THRIFT_PROTOCOL_FACTORY,
                 thrift_listen_queue_size=DEFAULT_THRIFT_LISTEN_QUEUE_SIZE,
                 worker_process_number=DEFAULT_WORKER_PROCESS_NUMBER,
                 harikiri=DEFAULT_HARAKIRI, tag=None, weight=None,
                 event_queue_size=DEFAULT_EVENT_QUEUE_SIZE,
                 thrift_recv_timeout=DEFAULT_THRIFT_RECV_TIMEOUT):
        # 父级启动文件路径
        boot_script_path = os.path.abspath((inspect.stack()[1])[1])
        # 根目录
        root_dir = os.path.dirname(boot_script_path)

        service_key = CommonUtil.get_service_key(thrift_module)
        # thrift processor
        processor = thrift_module.Processor(handler)
        port = int(port)
        if port == 0:
            port = CommonUtil.get_random_port()
        tag = CommonUtil.get_service_tag(tag)
        weight = CommonUtil.get_service_weight(weight)
        hostname = CommonUtil.get_hostname()
        self.service_config_data = ServiceConfigData(tag=tag, weight=weight, hostname=hostname)

        transport = ThriftServerSocket(queue=thrift_listen_queue_size, recv_timeout=thrift_recv_timeout, port=port)
        tfactory = TTransport.TFramedTransportFactory()
        pfactory = protocol_factory

    def start(self):
        pass
