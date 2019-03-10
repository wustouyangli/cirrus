# coding=utf-8

import time
import gevent
from util.schedule_task import ScheduleTask
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class ClientHolder(object):

    def __init__(self):
        self._client = None
        self._access_time = time.time()

    def get_client(self):
        return self._client

    def set_client(self, client):
        self._client = client

    def get_access_time(self):
        return self._access_time

    def set_access_time(self, access_time):
        self._access_time = access_time


class ClientPool(object):
    DEFAULT_CLIENT_EXPIRE_TIME = 300
    DEFAULT_CLOSE_EXPIRE_CLIENT_INTERVAL = 60

    def __init__(self, pool_name, pool_size, client_class, close_client_handler, *client_args, **client_kwargs):
        assert pool_size > 0
        assert client_class is not None and hasattr(client_class, '__call__')
        assert close_client_handler is None or hasattr(close_client_handler, '__call__')
        self._pool_name = pool_name
        self._pool_size = pool_size
        self._client_class = client_class
        self._close_client_handler = close_client_handler
        self._client_args = client_args
        self._client_kwargs = client_kwargs
        self._queue = gevent.queue.LifoQueue(maxsize=pool_size)
        for i in range(pool_size):
            self._queue.put(ClientHolder())
        self._client_expire_time = self.DEFAULT_CLIENT_EXPIRE_TIME
        self._gc_task = ScheduleTask(
            name='ClientPool-GC-%s' % pool_name,
            start_after_seconds=0,
            interval_seconds=self.DEFAULT_CLOSE_EXPIRE_CLIENT_INTERVAL,
            handler=self._close_expire_client
        )
        self._gc_task.run()

    @contextmanager
    def get_client(self, block=True, pool_acquire_client_timeout=1000, req_timeout=5000):
        client_holder = self._get_client(block, pool_acquire_client_timeout)
        tm = None
        try:
            tm = gevent.Timeout.start_new(req_timeout)
            yield client_holder.get_client()
        except Exception as e:
            logger.info('Client is out pool for too long %s seconds, raise exception: %s', req_timeout, e)
            self._close_client(client_holder)
            raise
        finally:
            if tm:
                tm.cancel()
            self.push(client_holder)

    def _get_client(self, block=True, timeout=1000):
        if self.is_empty():
            logger.info('ClientPool: %s is empty.', self._pool_name)
        client_holder = self._queue.get(block=block, timeout=timeout)
        if client_holder.get_client() is None:
            tm = None
            try:
                tm = gevent.Timeout.start_new(timeout)
                client_holder.set_client(self._create_client())
            except Exception as e:
                client_holder.set_client(None)
                self.push(client_holder)
                logger.info('Try get client from client pool %s raise exception: %s', self._pool_name, e)
                raise
            finally:
                if tm:
                    tm.cancel()
        client_holder.set_access_time(time.time())
        return client_holder

    def push(self, client_holder):
        if not self.is_full():
            self._queue.put_nowait(client_holder)

    def is_full(self):
        return self._queue.qsize() >= self._pool_size

    def is_empty(self):
        return self._queue.qsize() <= 0

    def _create_client(self):
        return self._client_class(*self._client_args, **self._client_kwargs)

    def _close_client(self, client_holder):
        if self._close_client_handler and client_holder.get_client():
            try:
                self._close_client_handler(client_holder.get_client())
            except Exception as e:
                logger.error('Close client raise exception: %s', e)
        client_holder.set_client(None)

    def _close_expire_client(self):
        cur_time = time.time()
        need_closed_clients = []
        for client_holder in self._queue.queue:
            if client_holder.get_client() and cur_time - client_holder.get_access_time() > self._client_expire_time:
                need_closed_clients.append(client_holder.get_client)

        for client in need_closed_clients:
            self._close_client_handler(client)
