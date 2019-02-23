# coding=utf-8

import os
import socket
import select
import signal
import errno
from thrift.server.TServer import TServer
from multiprocessing import Queue, Process, Manager, Value
from exception.harakiri_exception import HarakiriException
from thrift.transport import TTransport
from contextlib import contextmanager
from util.common_util import CommonUtil
from server.epoll_connection import EpollConnection, ConnectionStatus


class EpollServer(TServer):

    def __init__(self, *args, **kwargs):
        super(EpollServer, self).__init__(*args)
        self.clients = {}
        self.worker_processes = {}

        event_queue_size = kwargs.get('event_queue_size', 100)
        self.worker_process_number = kwargs.get('worker_process_number', 1)

        self.tasks = Queue(event_queue_size)
        """
        创建socket两端,thrift服务端处理完若干thrift客户端请求后,此socket write端将这些thrift客户端对应的文件描述符以
        分隔符','连接起来发送给read端,read端读取到这些文件描述符之后,依次将每个请求的响应发送给thrift客户端
        """
        self.read_side, self.write_side = socket.socketpair()
        self.stop_flag = Value('b', False)
        self.stop_read_flag = Value('b', False)
        self.epoll = select.epoll()
        # EPOLLIN设置读操作位
        self.epoll.register(self.read_side, select.EPOLLIN)

        self.harakiri = kwargs.get('harakiri', 50000)

        manager = Manager()
        self.responses = manager.dict()

    def register_harakiri(self):
        signal.signal(signal.SIGALRM, self.do_harakiri)

    def do_harakiri(self, signum, frame):
        raise HarakiriException('Execution killed after %s seconds' % self.harakiri)

    @contextmanager
    def harakiri_execute(self):
        signal.alarm(self.harakiri)
        try:
            yield
        finally:
            signal.alarm(0)

    def set_worker_process_number(self, num):
        self.worker_process_number = num

    def stop(self):
        self.stop_read_flag.vale = True
        # 做缓冲
        for put_count in range(self.worker_process_number):
            self.tasks.put([None, None])
        # close队列,需要看看这块的知识
        self.tasks.close()
        self.tasks.join_thread()
        self.stop_flag.value = True
        # 相当于close底层socket
        self.serverTransport.close()

    def serve(self):
        self.serverTransport.listen()
        self.serverTransport.handle.setblocking(0)
        # 注册thrift transport监视的文件描述符
        self.serverTransport.register(self.serverTransport.handle.fileno(), select.EPOLLIN)

        self.stop_flag.value = False
        self.stop_read_flag.value = False
        # fork工作进程
        for proc_no in range(self.worker_process_number):
            self.__fork_worker_process(proc_no)
        # 子进程异常终止重新fork
        signal.signal(signal.SIGCHLD, self.__refork_worker_process)

        while not self.stop_flag.value:
            try:
                self.handle()
            except (SystemExit, KeyboardInterrupt):
                break

    def __fork_worker_process(self, proc_no=0):
        process = Process(target=self.__start_worker_process, args=(proc_no,))
        process.start()
        self.worker_processes[proc_no] = process

    def __refork_worker_process(self, signum, frame):
        if not self.stop_flag.value:
            for proc_no, worker_process in self.worker_processes.iteritems():
                if not worker_process.is_alive():
                    self.__fork_worker_process(proc_no)

    def __start_worker_process(self, proc_no):
        self.register_harakiri()
        # 进程结束信号
        signal.signal(signal.SIGTERM, self.terminate_handler)
        while True:
            fileno = None
            try:
                # 从thrift客户端请求队列里取出客户端传送的数据和文件描述符
                message, fileno = self.tasks.get()
                itransport = TTransport.TMemoryBuffer(message)
                otransport = TTransport.TMemoryBuffer()
                iprot = self.inputProtocolFactory.getProtocol(itransport)
                oprot = self.outputProtocolFactory.getProtocol(otransport)

                if message is None:
                    break
                with self.harakiri_execute():
                    # thrift服务端处理请求
                    self.processor.process(iprot, oprot)
                # 保存文件描述符对应的客户端连接的响应数据
                self.responses[fileno] = (True, otransport.getvalue())
                # 将文件描述符以','分隔写入write端
                self.write_side.sendall(str(fileno) + ',')
            except Exception:
                if fileno:
                    self.responses[fileno] = (False, b'')
                    self.write_side.sendall(str(fileno) + ',')

    def terminate_handler(self, signum, frame):
        raise SystemExit()

    def handle(self):
        try:
            events = self.epoll.poll(1)
        except Exception as e:
            # 慢系统调用异常
            if CommonUtil.get_exception_errno(e) == errno.EINTR:
                events = []
            else:
                raise

        for fileno, event in events:
            if fileno == self.serverTransport.handle.fileno() and not self.stop_read_flag.value:
                # 获取到thrift客户端连接
                client = self.serverTransport.accept().handle
                self.clients[client.fileno()] = EpollConnection(client, self.epoll)
                # epoll注册客户端对应的文件描述符
                self.register_epollin(client.fileno())
            elif event & select.EPOLLIN:
                if fileno == self.read_side.fileno():
                    msg = self.read_side.recv(1024)
                    for client_fileno in msg.split(',')[:-1]:
                        if client_fileno == '' or client_fileno is None:
                            continue
                        client_fileno = int(client_fileno)
                        connection = self.clients.get(client_fileno)
                        response = self.responses.get(client_fileno)
                        if connection and response:
                            connection.ready(*response)
                elif not self.stop_read_flag.value:
                    connection = self.clients.get(fileno)
                    if connection:
                        connection.read()
                        if connection.status == ConnectionStatus.WAIT_PROCESS:
                            pass

    def register_epollin(self, fileno):
        self.epoll.register(fileno, select.EPOLLIN)

    def register_epollout(self, fileno):
        self.epoll.register(fileno, select.EPOLLOUT)





