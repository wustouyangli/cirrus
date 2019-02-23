# coding=utf-8

import env_base
import os
import sys
import socket
import struct
from util.common_util import CommonUtil


def test_get_random_port():
    port = CommonUtil.get_random_port()
    print port


def test_get_hostname():
    hostname = CommonUtil.get_hostname()
    print hostname


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'get_random_port':
        test_get_random_port()
    elif len(sys.argv) == 2 and sys.argv[1] == 'get_hostname':
        test_get_hostname()
