# coding=utf-8

import time
import gevent
import logging

logger = logging.getLogger(__name__)


class ScheduleTask(object):
    """
    定时任务, 调用之前需使用gevent.monkey.patch_all()
    @:param name: 任务名
    @:param start_after_seconds: 延迟执行时间
    @:param interval_seconds: 执行时间间隔
    @:param handler: 执行方法
    @:param args: 执行方法参数
    @:param kwargs: 执行方法参数
    """

    def __init__(self, name, start_after_seconds, interval_seconds, handler, *args, **kwargs):
        assert handler is not None and hasattr(handler, '__call__')
        self._name = name
        self._start_after_seconds = start_after_seconds
        self._interval_seconds = interval_seconds
        self._handler = handler
        self._args = args
        self._kwargs = kwargs
        self._last_schedule_time = time.time() + start_after_seconds - interval_seconds
        self._task = None

    def run(self):
        self._task = gevent.spawn(self._run)

    def _run(self):
        try:
            while True:
                cur_time = time.time()
                if cur_time - self._interval_seconds >= self._last_schedule_time:
                    try:
                        self._handler(*self._args, **self._kwargs)
                        self._last_schedule_time = time.time()
                    except gevent.GreenletExit:
                        raise
                    except Exception as e:
                        logger.error('Schedule task %s catch exception: %s', self._name, e)

                sleep_seconds = self._last_schedule_time + self._interval_seconds - time.time()
                if sleep_seconds < 0:
                    sleep_seconds = 0
                gevent.sleep(sleep_seconds)
        except gevent.GreenletExit:
            logger.error("Schedule task %s unexpected exit." % self._name)

    def stop(self):
        if self._task:
            self._task.kill(block=True)
            logger.info("Schedule task %s stopped." % self._name)
