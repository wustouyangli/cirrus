# coding=utf-8

import sys
import struct
import errno
import socket
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TTransportException


class ThriftSocket(TSocket):

    def __init__(self, recv_timeout=50000, **kwargs):
        super(ThriftSocket, self).__init__(**kwargs)
        self.recv_timeout = recv_timeout

    def read(self, sz):
        buff = ''
        for try_count in range(3):
            try:
                # 设置接收数据超时时间
                self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, struct.pack('LL', 0, self.recv_timeout))
                buff = self.handle.recv(sz)
                # self.handle.settimeout(None)
            except socket.error as e:
                if (e.args[0] == errno.ECONNRESET and
                        (sys.platform == 'darwin' or sys.platform.startswith('freebsd'))):
                    # freebsd and Mach don't follow POSIX semantic of recv
                    # and fail with ECONNRESET if peer performed shutdown.
                    # See corresponding comment and code in TSocket::read()
                    # in lib/cpp/src/transport/TSocket.cpp.
                    self.close()
                    # Trigger the check to raise the END_OF_FILE exception below.
                    buff = ''

                elif e.args[0] == errno.EDEADLK:
                    # 资源死锁
                    buff = ''
                else:
                    raise
        if len(buff) == 0:
            raise TTransportException(type=TTransportException.END_OF_FILE,
                                      message='TSocket read 0 bytes')
        return buff

