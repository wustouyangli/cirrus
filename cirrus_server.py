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
from server.instance_config_data import InstanceConfigData
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
        self._instance_config_data = InstanceConfigData(tag=tag, weight=weight, hostname=hostname)

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
            instance_config_data=self._instance_config_data,
            port=port
        )

        self._service_key = service_key
        self._port = port
        self._server = server
        self._tag = tag
        self._handler = handler
        self._worker_process = None
        self._local_ip = CommonUtil.get_local_ip()
        instance_path = '%s:%s' % (self._local_ip, self._port)
        self._zk_publisher = ZkPublisher(self._service_key, instance_path)

    def start(self):
        try:
            logger.info('Master process id: %d', os.getpid())
            CommonUtil.set_proctitle('master')
            # 启动工作进程
            worker_process = Process(target=self._start)
            worker_process.start()
            logger.info('Worker process id: %d, port: %d', worker_process.pid, self._port)
            self._worker_process = worker_process

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
            logger.error('Master process id: %d error: %s', os.getpid(), e.message)
            self._stop(exit_code=1)

    def _start(self):
        # SIG_IGN忽略子进程状态信息,子进程会被自动回收,不会产生僵尸进程
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        CommonUtil.set_proctitle('worker')
        self._server.serve()

    def _signal_exit(self, signum, frame):
        logger.info('Master process id: %d receive signal %d', os.getpid(), signum)
        if signum == signal.SIGINT:
            self._stop(graceful=False)
        else:
            self._stop(graceful=True)

    def _stop(self, exit_code=0, graceful=True):
        logger.info('Master process id: %d stop worker process: %d', os.getpid(), self._worker_process.pid)
        self._unregister_server()
        if graceful:
            time.sleep(CommonUtil.get_sec_for_server_teardown())
        self._server.stop()
        self._worker_process.terminate()
        self._worker_process.join()
        sys.exit(exit_code)

    def _unregister_server(self):
        self._zk_publisher.stop()

    def _register_server(self):
        self._zk_publisher.register(self._instance_config_data)
