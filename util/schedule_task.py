# coding=utf-8

import time
import gevent
import logging

logger = logging.getLogger(__name__)


class ScheduleTask(object):

    def __init__(self, name, start_after_seconds, interval_seconds, handler, *args, **kwargs):
        assert handler is not None and hasattr(handler, '__call__')
        self._name = name
        self._start_after_seconds = start_after_seconds
        self._interval_seconds = interval_seconds
        self._handler = handler
        self._args = args
        self._kwargs = kwargs
        self._task = None
        self._last_schedule_time = time.time() + start_after_seconds - interval_seconds

    def run(self):
        self._task = gevent.spawn(self._run())

    '''
    无限循环执行
    每隔一段时间执行一次
    在下一次执行之前休眠一段时间
    '''
    def _run(self):
        while True:
            cur_time = time.time()
            if cur_time - self._interval_seconds >= self._last_schedule_time:
                try:
                    self._handler(*self._args, **self._kwargs)
                    self._last_schedule_time = time.time()
                except Exception as e:
                    logger.error('Schedule task %s raise exception: %s', self._name, repr(e))

            sleep_seconds = self._last_schedule_time + self._interval_seconds - time.time()
            if sleep_seconds < 0:
                sleep_seconds = 0
            gevent.sleep(sleep_seconds)

    def stop(self):
        if self._task:
            self._task.kill(block=True)
