# coding=utf-8

import time
import logging
import random
logger = logging.getLogger(__name__)


class HostSelector(object):

    def __init__(self, host_provider, use_weight=False, expire_time=600, retry_time=60):
        self._host_provider = host_provider
        self._use_weight = use_weight
        self._expire_time = expire_time
        self._retry_time = retry_time
        self._bad_hosts = {}
        self._selected_time = None
        self._selected_host = None
        self._last_selected_host = None

    def get_host(self):
        cur_time = time.time()

        for host, marked_time in self._bad_hosts.items():
            if cur_time - marked_time > self._retry_time:
                logger.info('Remove %s from bad hosts over retry time', host)
                del self._bad_hosts[host]

        select_another = False
        if self._selected_time is None or cur_time - self._selected_time > self._expire_time:
            select_another = True

        if select_another:
            self._last_selected_host = self._selected_host
            self._selected_host = self._select_host()
            self._selected_time = cur_time

        return self._selected_host

    def _select_host(self):
        if self._use_weight:
            return self._weighted_select_host()
        else:
            return self._random_select_host()

    def _random_select_host(self):
        instances = self._host_provider.get_instances()
        assert len(instances)
        for i in range(3):
            instance = random.choice(instances)
            if instance.host not in self._bad_hosts.keys():
                return instance.host

        good_hosts = [instance.host for instance in instances if instance.host not in self._bad_hosts.keys()]
        assert len(good_hosts)
        host = random.choice(good_hosts)
        return host

    def _weighted_select_host(self):
        instances = self._host_provider.get_instances()
        good_instances = [instance for instance in instances if instance.host not in self._bad_hosts.keys()]
        assert len(good_instances)
        total_weight = sum(instance.instance_config_data.weight for instance in instances)
        value = random.random() * total_weight

        for instance in good_instances:
            if value < instance.instance_config_data.weight:
                return instance.host
            else:
                value -= instance.instance_config_data.weight

        return good_instances[0].host

    def invalid_host(self):
        if self._selected_host not in self._bad_hosts:
            cur_time = time.time()
            self._bad_hosts[self._selected_host] = cur_time
            logger.info('Invalid host: %s at %s', self._selected_host, cur_time)

        self._last_selected_host = self._selected_host
        self._selected_host = None
        self._selected_time = None
