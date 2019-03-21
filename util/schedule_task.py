# coding=utf-8

import time
import ctypes
import inspect
import logging
import threading

logger = logging.getLogger(__name__)


class ScheduleTask(object):
    """
    定时任务
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
        self._task = threading.Thread(target=self._run)
        self._task.setDaemon(True)
        self._task.start()
    
    def _run(self):
        try:
            while True:
                cur_time = time.time()
                if cur_time - self._interval_seconds >= self._last_schedule_time:

                    self._handler(*self._args, **self._kwargs)
                    self._last_schedule_time = time.time()

                sleep_seconds = self._last_schedule_time + self._interval_seconds - time.time()
                if sleep_seconds < 0:
                    sleep_seconds = 0
                time.sleep(sleep_seconds)
        except Exception as e:
            logger.error("Schedule task %s unexpected exit, exception: %s" % self._name, e)
            self.stop()

    def _async_raise(self, tid, exctype):
        # raises the exception, performs cleanup if needed
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def force_stop(self, thread):
        self._async_raise(thread.ident, SystemExit)

    def stop(self):
        if self._task:
            self._task.join()
            time.sleep(1)
            if self._task.is_alive():
                self.force_stop(self._task)
            logger.info("Schedule task %s stopped." % self._name)
