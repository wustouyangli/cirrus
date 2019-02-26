# coding=utf-8

import os
import sys
import inspect
import logging
import signal
import time
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from util.common_util import CommonUtil
from server.service_config_data import ServiceConfigData
from transport.thrift_server_socket import ThriftServerSocket
from server.epoll_server import EpollServer
from multiprocessing import Process
from zookeeper.zk_publisher import ZkPublisher

logger = logging.getLogger(__name__)


class CirrusServer(object):

    DEFAULT_THRIFT_PROTOCOL_FACTORY = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()
    DEFAULT_THRIFT_LISTEN_QUEUE_SIZE = 128
    DEFAULT_WORKER_PROCESS_NUMBER = 4
    DEFAULT_UNPROD_WORKER_PROCESS_NUMBER = 1
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

        if not CommonUtil.is_prod():
            worker_process_number = self.DEFAULT_UNPROD_WORKER_PROCESS_NUMBER

        server = EpollServer(
            processor,
            transport,
            tfactory,
            pfactory,
            harakiri=harikiri,
            event_queue_size=event_queue_size,
            worker_process_number=worker_process_number,
            service_config_data=self.service_config_data,
            port=port
        )

        self.service_key = service_key
        self.port = port
        self.server = server
        self.tag = tag
        self.handler = handler
        self.worker_process = None
        self.local_ip = CommonUtil.get_local_ip()
        instance_path = '%s/%s:%s' % (self.service_key, self.local_ip, self.port)
        self.zk_publisher = ZkPublisher(instance_path)

    def start(self):
        try:
            logger.info('master process id: %d', os.getpid())
            # 启动工作进程
            worker_process = Process(target=self._start)
            worker_process.start()
            logger.info('worker process id: %d, port: %d', worker_process.pid, self.port)
            self.worker_process = worker_process

            # kill命令不加参数终止进程
            signal.signal(signal.SIGTERM, self._signal_exit)
            # ctrl + C 终止进程
            signal.signal(signal.SIGINT, self._signal_exit)
            # 段错误,try catch难以捕获此异常
            signal.signal(signal.SIGSEGV, self._signal_exit)
            # zookeeper注册服务
            self._register_server()
            # 工作进程异常退出
            worker_process.join()
            logger.error('Worker process id: %d unexpected exit', worker_process.pid)
        except Exception as e:
            # 主进程异常退出
            logger.error('master process id: %d error: %s', os.getpid(), e.message)
            self._stop(exit_code=1)

    def _start(self):
        # SIG_IGN忽略子进程状态信息,子进程会被自动回收,不会产生僵尸进程
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        self.server.serve()

    def _signal_exit(self, signum, frame):
        logger.info('master process id: %d receive signal %d', os.getpid(), signum)
        if signum == signal.SIGINT:
            self._stop(graceful=False)
        else:
            self._stop(graceful=True)

    def _stop(self, exit_code=0, graceful=True):
        logger.info('master process id: %d stop worker process: %d', os.getpid(), self.worker_process.pid)
        self._unregister_server()
        if graceful:
            time.sleep(CommonUtil.get_sec_for_server_teardown())
        self.server.stop()
        sys.exit(exit_code)

    def _unregister_server(self):
        self.zk_publisher.stop()

    def _register_server(self):
        self.zk_publisher.register(self.service_config_data)
