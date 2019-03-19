# coding=utf-8

import os
import errno
import socket
from thrift.transport.TSocket import TServerSocket
from transport.thrift_socket import ThriftSocket


class ThriftServerSocket(TServerSocket):

    # 对thrift进行了封装,可定制监听大小和接收数据的超时时间
    def __init__(self, queue=128, recv_timeout=50000, **kwargs):
        super(ThriftServerSocket, self).__init__(**kwargs)
        self.queue = queue
        self.recv_timeout = recv_timeout

    def listen(self):
        res0 = self._resolveAddr()
        socket_family = self._socket_family == socket.AF_UNSPEC and socket.AF_INET6 or self._socket_family
        for res in res0:
            if res[0] is socket_family or res is res0[-1]:
                break

        # We need remove the old unix socket if the file exists and
        # nobody is listening on it.
        if self._unix_socket:
            tmp = socket.socket(res[0], res[1])
            try:
                tmp.connect(res[4])
            except socket.error as err:
                eno, message = err.args
                if eno == errno.ECONNREFUSED:
                    os.unlink(res[4])

        self.handle = socket.socket(res[0], res[1])
        self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(self.handle, 'settimeout'):
            self.handle.settimeout(None)
        self.handle.bind(res[4])
        self.handle.listen(self.queue)

    def accept(self):
        client, addr = self.handle.accept()
        result = ThriftSocket(self.recv_timeout)
        result.setHandle(client)
        return result
