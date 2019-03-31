# coding=utf-8

import env_base
import logging
from calculator import CalculatorService
from calculator.ttypes import ResultResponse
from calculator.CalculatorService import Client
from cirrus_server import CirrusServer
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(asctime)s - %(name)s %(process)d - %(message)s')

logger = logging.getLogger(__name__)


class CalculatorHandler(Client):
    def __init__(self):
        pass

    def calculate(self, op, a, b):
        if op == 'add':
            return ResultResponse(result=a + b)
        elif op == 'sub':
            return ResultResponse(result=a - b)
        return ResultResponse(result=0)


def test_cirrus_server():
    thrift_module = CalculatorService
    handler = CalculatorHandler()
    cirrus_server = CirrusServer(thrift_module, handler)
    cirrus_server.start()


if __name__ == "__main__":
    test_cirrus_server()
