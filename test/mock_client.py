# coding=utf-8

import env_base
import gevent
from gevent import monkey

# monkey.patch_all()

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s')

from cirrus_client import CirrusClient
from oyl_thrift.gen_py.com.oyl import OylWorkService


def test_cirrus_client():
    thrift_module = OylWorkService

    client = CirrusClient(thrift_module, pool_size=5)

    def f(client):
        op = 'sub'
        a = 10
        b = 2
        res = client.work(op, a, b)
        print res.result

    jobs = []
    for i in range(1):
        jobs.append(gevent.spawn(f, client))

    gevent.joinall(jobs)


if __name__ == "__main__":
    test_cirrus_client()
