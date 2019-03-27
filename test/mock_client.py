# coding=utf-8

import env_base
from gevent import monkey
import gevent
import logging
from cirrus_client import CirrusClient
from calculator import CalculatorService
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s')


def test_cirrus_client():
    thrift_module = CalculatorService

    client = CirrusClient(thrift_module, pool_size=5)

    def f(client):
        op = 'sub'
        a = 10
        b = 2
        res = client.calculate(op, a, b)
        print res.result

    jobs = []
    for i in range(1):
        jobs.append(gevent.spawn(f, client))

    gevent.joinall(jobs)


if __name__ == "__main__":
    test_cirrus_client()
