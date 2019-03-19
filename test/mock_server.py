# coding=utf-8

import env_base

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s')

from oyl_thrift.gen_py.com.oyl import OylWorkService
from oyl_thrift.gen_py.com.oyl.ttypes import Work
from cirrus_server import CirrusServer


class workHandler(object):
    def __init__(self):
        pass

    def work(self, op, a, b):
        if op == 'add':
            return Work(result=a + b)
        elif op == 'sub':
            return Work(result=a - b)
        return Work(result=0)


def test_cirrus_server():
    thrift_module = OylWorkService
    handler = workHandler()
    cirrus_server = CirrusServer(thrift_module, handler)
    cirrus_server.start()


if __name__ == "__main__":
    test_cirrus_server()
