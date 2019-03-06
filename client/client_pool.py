# coding=utf-8

import time
from gevent.queue import LifoQueue
'''
连接池
'''


class ClientHolder(object):

    def __init__(self):
        self.client = None
        self.access_time = time.time()

    def set_client(self, client):
        self.client = client

    def set_access_time(self, access_time):
        self.access_time = access_time


class ClientPool(object):

    def __init__(self, pool_size, client_class, close_client_handler, *client_args, **client_kwargs):
        assert pool_size > 0
        assert client_class is not None and hasattr(client_class, '__call__')
        assert close_client_handler is None or hasattr(close_client_handler, '__call__')
        self._pool_size = pool_size
        self._client_class = client_class
        self._close_client_handler = close_client_handler
        self._client_args = client_args
        self._client_kwargs = client_kwargs
        self._queue = LifoQueue(maxsize=pool_size)
        for i in range(pool_size):
            self._queue.put(ClientHolder())



    '''
    获取单个连接
    block: 是否以阻塞的方式获取连接,为True时,队列为空一直等待, 否则直接返回
    timeout: 等待超时时间 
    '''
    def get_client(self, block=True, timeout=1000):
        pass

    def is_full(self):
        return

    def is_empty(self):
        return