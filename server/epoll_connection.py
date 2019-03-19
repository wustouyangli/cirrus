# coding=utf-8

import socket
import struct
import select
import logging

logger = logging.getLogger(__name__)


class ConnectionStatus(object):
    WAIT_LEN = 0      # 等待消息长度
    WAIT_MSG = 1      # 等待消息
    WAIT_PROCESS = 2  # 等待处理
    PROCESSING = 3    # 正在处理
    CLOSED = 4        # 关闭连接


def socket_exception_wrapper(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except socket.error:
            self.close()
    return wrapper


class EpollConnection(object):
    def __init__(self, new_socket, epoll):
        self._socket = new_socket
        self._socket.setblocking(False)
        self._epoll = epoll

        self._status = ConnectionStatus.WAIT_LEN
        self._msg_len = 0
        self._msg = b''

    def get_msg(self):
        return self._msg

    def get_fileno(self):
        return self._socket.fileno()

    def get_status(self):
        return self._status

    def ready(self, succeed, resp_msg):
        assert self._status == ConnectionStatus.WAIT_PROCESS
        if not succeed:
            self.close()
            return

        self._msg_len = 0
        if len(resp_msg) == 0:
            self._msg = b''
            self._status = ConnectionStatus.WAIT_LEN  # 等待消息长度
        else:
            self._msg = struct.pack('!i', len(resp_msg)) + resp_msg
            self._status = ConnectionStatus.PROCESSING  # 处理中
            self._modify_epoll(self._socket.fileno(), select.EPOLLOUT)

    def _read_len(self):
        s = self._socket.recv(4 - len(self._msg))
        if len(s) == 0:
            self.close()
            return

        self._msg += s
        if len(self._msg) == 4:
            self._msg_len, = struct.unpack('!i', self._msg)
            if self._msg_len <= 0:
                self.close()
            else:
                self._msg = b''
                self._status = ConnectionStatus.WAIT_MSG

    @socket_exception_wrapper
    def read(self):
        assert self._status in [ConnectionStatus.WAIT_LEN, ConnectionStatus.WAIT_MSG]
        if self._status == ConnectionStatus.WAIT_LEN:
            self._read_len()
        elif self._status == ConnectionStatus.WAIT_MSG:
            s = self._socket.recv(self._msg_len - len(self._msg))
            if len(s) == 0:
                self.close()
                return
            self._msg += s
            # 读取消息完整时才变成待处理状态
            if len(self._msg) == self._msg_len:
                self._status = ConnectionStatus.WAIT_PROCESS
                self._modify_epoll(self._socket.fileno(), 0)

    @socket_exception_wrapper
    def write(self):
        assert self._status == ConnectionStatus.PROCESSING
        send_size = self._socket.send(self._msg)
        # 消息发送完毕
        if send_size == len(self._msg):
            self._status = ConnectionStatus.WAIT_LEN
            self._msg = b''
            self._msg_len = 0
            # 变成读入状态
            self._modify_epoll(self._socket.fileno(), select.EPOLLIN)
        else:
            self._msg = self._msg[send_size:]

    def close(self):
        self._status = ConnectionStatus.CLOSED
        self._unregister_epoll(self._socket.fileno())
        self._socket.close()

    def reset(self):
        self._status = ConnectionStatus.CLOSED
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        self._unregister_epoll(self._socket.fileno())
        self._socket.close()

    # 取消注册文件描述符
    def _unregister_epoll(self, fileno):
        self._epoll.unregister(fileno)

    # 修改文件描述符状态
    def _modify_epoll(self, fileno, status):
        self._epoll.modify(fileno, status)


