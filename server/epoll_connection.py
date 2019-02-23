# coding=utf-8

import socket


class ConnectionStatus(object):
    WAIT_REQ = 0      # 等待请求
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
        self.socket = new_socket
        self.socket.setblocking(False)
        self.epoll = epoll

        self.status = ConnectionStatus.WAIT_REQ
        self.msg_len = 0
        self.msg = b''

    def ready(self, *args):
        pass

    @socket_exception_wrapper
    def read(self):
        pass

    @socket_exception_wrapper
    def write(self):
        pass

    def close(self):
        pass

    def reset(self):
        pass

    # 取消注册文件描述符
    def unregister_epoll(self, fileno):
        self.epoll.unregister(fileno)

    # 修改文件描述符状态
    def modify_epoll(self, fileno, status):
        self.epoll.modify(fileno, status)


