# coding=utf-8

import time
import random
import logging

logger = logging.getLogger(__name__)


class LimiterStatus(object):
    OPEN = 'open'            # 开启限流
    STABLE = 'stable'        # 平稳状态
    CLOSED = 'close'         # 关闭限流

    all = [OPEN, STABLE, CLOSED]


class ConnectionLimiter(object):
    DEFAULT_WARN_CHECK_RATE = 0.1
    DEFAULT_OPEN_TIME = 3
    DEFAULT_STABLE_TIME = 5
    DEFAULT_TRY_CLOSE_TIME = 5

    def __init__(self, size_handler, capability):
        # 初始限流是关闭的
        self._status = LimiterStatus.CLOSED
        self._capability = capability
        self._low_level_size = int(capability * 0.3)
        self._warn_level_size = int(capability * 0.5)
        self._high_level_size = int(capability * 0.8)
        self._size_handler = size_handler
        self._warn_count = 0
        self._current_warn_check_rate = self.DEFAULT_WARN_CHECK_RATE
        self._status_start_time = time.time()
        random.seed()

    def try_acquire(self):
        size = self.__get_size()
        if size >= self._capability:
            return False

        if self.is_closed():
            self.__try_open()
            return True

        now = time.time()

        if self.is_open():
            if size <= self._warn_level_size:
                self.__set_status(LimiterStatus.STABLE)
            else:
                pass_time = now - self._status_start_time
                limit_rate = 1.0 - pass_time * 1.0 / self.DEFAULT_OPEN_TIME
                r = random.random()
                return r <= limit_rate

        if self.is_stable():
            pass_time = now - self._status_start_time
            if size <= self._low_level_size or pass_time > self.DEFAULT_STABLE_TIME:
                self.__set_status(LimiterStatus.CLOSED)
            else:
                limit_rate = 1.0 - (pass_time * 1.0 / self.DEFAULT_OPEN_TIME) ** 2
                r = random.random()
                return r <= limit_rate
        
        return True

    def __get_size(self):
        return self._size_handler()

    def is_open(self):
        return self._status == LimiterStatus.OPEN

    def is_stable(self):
        return self._status == LimiterStatus.STABLE

    def is_closed(self):
        return self._status == LimiterStatus.CLOSED

    def __set_status(self, status):
        if status in LimiterStatus.all:
            pre_status = self._status
            self._status = status
            now = time.time()
            logger.info('Limiter changes status from %s to %s at %s', pre_status, self._status, now)
            self._status_start_time = now

    def __try_open(self):
        size = self.__get_size()
        if size <= self._warn_level_size:
            # 低水位时不做检查
            self.__init_warn_check()
            return
        elif self._warn_level_size < size <= self._high_level_size:
            # 超过警戒水位低于高水位时抽样计数,达到5次则开启限流
            if (size - self._warn_level_size) * 1.0 / self._warn_level_size > self._current_warn_check_rate:
                self._current_warn_check_rate *= 2
            r = random.random()
            if r > self._current_warn_check_rate:
                return
            self._warn_count += 1
            if self._warn_count >= 5:
                logger.info('The size over warn level size more than %s times in check point, open the limiter', self._warn_count)
                self.__init_warn_check()
                self.__set_status(LimiterStatus.OPEN)
        else:
            # 高于高水位直接开启限流
            logger.info('The size over high level size, open the limiter')
            self.__init_warn_check()
            self.__set_status(LimiterStatus.OPEN)

    def __init_warn_check(self):
        self._warn_count = 0
        self._current_warn_check_rate = self.DEFAULT_WARN_CHECK_RATE
