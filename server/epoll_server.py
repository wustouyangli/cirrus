# coding=utf-8

import os
import socket
import select
import signal
import errno
import Queue as _Queue
from thrift.server.TServer import TServer
from multiprocessing import Queue, Process, Manager, Value
from thrift.transport import TTransport
from contextlib import contextmanager
from util.common_util import CommonUtil
from server.epoll_connection import EpollConnection, ConnectionStatus
from server.connection_limiter import ConnectionLimiter


class EpollServer(TServer):

    def __init__(self, *args, **kwargs):
        super(EpollServer, self).__init__(*args)
        self._clients = {}
        self._worker_processes = {}

        event_queue_size = kwargs.get('event_queue_size', 100)
        self._worker_process_number = kwargs.get('worker_process_number', 1)

        self._tasks = Queue(event_queue_size)
        """
        创建socket两端,thrift服务端处理完若干thrift客户端请求后,此socket write端将这些thrift客户端对应的文件描述符以
        分隔符','连接起来发送给read端,read端读取到这些文件描述符之后,依次将每个请求的响应发送给thrift客户端
        """
        self._read_side, self._write_side = socket.socketpair()
        self._stop_flag = Value('b', False)
        self._stop_read_flag = Value('b', False)
        self._epoll = select.epoll()
        # EPOLLIN设置读操作位
        self._epoll.register(self._read_side, select.EPOLLIN)

        self._harakiri = kwargs.get('harakiri', 5)

        manager = Manager()
        self._responses = manager.dict()
        self._connection_limiter = ConnectionLimiter(self._get_queue_size, event_queue_size)

    def _get_queue_size(self):
        try:
            return self._tasks.qsize()
        except NotImplementedError:
            return 0

    def _register_harakiri(self):
        signal.signal(signal.SIGALRM, self._do_harakiri)

    def _do_harakiri(self, signum, frame):
        raise Exception('Execution killed after %s seconds' % self._harakiri)

    @contextmanager
    def _harakiri_execute(self):
        signal.alarm(self._harakiri)
        try:
            yield
        finally:
            signal.alarm(0)

    def set_worker_process_number(self, num):
        self._worker_process_number = num

    def stop(self):
        self._stop_read_flag.vale = True
        # 做缓冲
        for put_count in range(self._worker_process_number):
            self._tasks.put([None, None])
        # close队列,需要看看这块的知识
        self._tasks.close()
        self._tasks.join_thread()
        self._stop_flag.value = True
        # 相当于close底层socket
        self.serverTransport.close()

    def serve(self):
        self.serverTransport.listen()
        self.serverTransport.handle.setblocking(0)
        # 注册thrift transport监视的文件描述符
        self.serverTransport.register(self.serverTransport.handle.fileno(), select.EPOLLIN)

        self._stop_flag.value = False
        self._stop_read_flag.value = False
        # fork工作进程
        for proc_no in range(self._worker_process_number):
            self._fork_worker_process(proc_no)
        # 子进程异常终止重新fork
        signal.signal(signal.SIGCHLD, self._refork_worker_process)

        while not self._stop_flag.value:
            try:
                self.handle()
            except (SystemExit, KeyboardInterrupt):
                break

    def _fork_worker_process(self, proc_no=0):
        process = Process(target=self._start_worker_process, args=(proc_no,))
        process.start()
        self._worker_processes[proc_no] = process

    def _refork_worker_process(self, signum, frame):
        if not self._stop_flag.value:
            for proc_no, worker_process in self._worker_processes.iteritems():
                if not worker_process.is_alive():
                    self._fork_worker_process(proc_no)

    def _start_worker_process(self, proc_no):
        self._register_harakiri()
        # 进程结束信号
        signal.signal(signal.SIGTERM, self._terminate_handler)
        while True:
            fileno = None
            try:
                # 从thrift客户端请求队列里取出客户端传送的数据和文件描述符
                message, fileno = self._tasks.get()
                itransport = TTransport.TMemoryBuffer(message)
                otransport = TTransport.TMemoryBuffer()
                iprot = self.inputProtocolFactory.getProtocol(itransport)
                oprot = self.outputProtocolFactory.getProtocol(otransport)

                if message is None:
                    break
                with self._harakiri_execute():
                    # thrift服务端处理请求
                    self.processor.process(iprot, oprot)
                # 保存文件描述符对应的客户端连接的响应数据
                self._responses[fileno] = (True, otransport.getvalue())
                # 将文件描述符以','分隔写入write端
                self._write_side.sendall(str(fileno) + ',')
            except Exception:
                if fileno:
                    self._responses[fileno] = (False, b'')
                    self._write_side.sendall(str(fileno) + ',')

    def _terminate_handler(self, signum, frame):
        raise SystemExit()

    def handle(self):
        try:
            events = self._epoll.poll(1)
        except Exception as e:
            # 慢系统调用异常
            if CommonUtil.get_exception_errno(e) == errno.EINTR:
                events = []
            else:
                raise

        for fileno, event in events:
            if fileno == self.serverTransport.handle.fileno() and not self._stop_read_flag.value:
                # 获取到thrift客户端连接
                client = self.serverTransport.accept().handle
                self._clients[client.fileno()] = EpollConnection(client, self._epoll)
                # epoll注册客户端对应的文件描述符
                self.register_epollin(client.fileno())
            elif event & select.EPOLLIN:
                if fileno == self._read_side.fileno():
                    msg = self._read_side.recv(1024)
                    for client_fileno in msg.split(',')[:-1]:
                        if client_fileno == '' or client_fileno is None:
                            continue
                        client_fileno = int(client_fileno)
                        connection = self._clients.get(client_fileno)
                        response = self._responses.get(client_fileno)
                        if connection and response:
                            connection.ready(*response)
                elif not self._stop_read_flag.value:
                    connection = self._clients.get(fileno)
                    if connection:
                        connection.read()
                        if connection.status == ConnectionStatus.WAIT_PROCESS:
                            try:
                                if self._connection_limiter.try_acquire():
                                    self._tasks.put_nowait([connection.get_msg(), connection.get_flieno()])
                                else:
                                    connection.reset()
                                    del self._clients[fileno]
                            except _Queue.Full:
                                connection.reset()
                                del self._clients[fileno]
                else:
                    connection = self._clients[fileno]
                    connection.reset()
                    del self._clients[fileno]
            elif events & select.EPOLLOUT:
                connection = self._clients.get(fileno)
                if connection:
                    connection.write()
            elif events & select.EPOLLHUP:
                connection = self._clients.get(fileno)
                if connection:
                    connection.close()
                    del self._clients[fileno]

    def register_epollin(self, fileno):
        self._epoll.register(fileno, select.EPOLLIN)

    def register_epollout(self, fileno):
        self._epoll.register(fileno, select.EPOLLOUT)





