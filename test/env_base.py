# coding=utf-8

import os
import sys

sub_dirs = [
    ''
]


def include_path():
    path = '/'.join(os.path.abspath(__file__).split('/')[:-2])
    for sub_dir in sub_dirs:
        sys.path += [path + sub_dir]


include_path()
