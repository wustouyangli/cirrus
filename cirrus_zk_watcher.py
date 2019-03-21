# coding=utf-8

import argparse
import logging
from zookeeper.zk_watcher import ZkWatcher
parser = argparse.ArgumentParser(description='Zookeeper watcher for ensuring zk data copy to local')
parser.add_argument('--debug', action='store_true', help='enable debug')
parser.add_argument('-d', action='store_true', help='run in daemon mode')


if __name__ == "__main__":
    args = parser.parse_args()

    daemon = bool(args.d)
    log_level = logging.DEBUG if bool(args.debug) else logging.INFO
    zk_watcher = ZkWatcher(daemon=daemon, log_level=log_level)
    zk_watcher.watch()


