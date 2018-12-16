# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import sys

from CDaemon import CDaemon
from callback import callbackworker
from rabbitmq import RabbitConsumerAysnc


class RabbitDaemon(CDaemon):
    def __init__(self, name, save_path, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, home_dir='.', umask=022, verbose=1):
        CDaemon.__init__(self, save_path, stdin, stdout, stderr, home_dir, umask, verbose)
        self.name = name  # 派生守护进程类的名称

    def run(self, output_fn, **kwargs):
        # 新建一个队列链接
        rab_con = RabbitConsumerAysnc(queuename="fish_test", callbackworker=callbackworker, prefetch_count=2)
        # 开启消费者进程
        rab_con.run()


if __name__ == "__main__":
    p_name = 'fish_test'  # 守护进程名称
    pid_fn = '/home/daemon_fish_test.pid'  # 守护进程pid文件的绝对路径
    log_fn = '/home/daemon_fish_test.log'  # 守护进程日志文件的绝对路径
    err_fn = '/home/daemon_fish_test.err.log'  # 守护进程启动过程中的错误日志,内部出错能从这里看到
    cD = RabbitDaemon(p_name, pid_fn, stderr=err_fn, verbose=1)

    if len(sys.argv) == 2:
        if sys.argv[1] == 'start':
            cD.start(log_fn)
        elif sys.argv[1] == 'stop':
            cD.stop()
        elif sys.argv[1] == 'restart':
            cD.restart(log_fn)
        elif sys.argv[1] == 'status':
            alive = cD.is_running()
            if alive:
                print 'process [%s] is running ......' % cD.get_pid()
            else:
                print 'daemon process [%s] stopped' % cD.name
        else:
            print 'Usage: python %s <start|stop|restart|status>' % sys.argv[0]
            sys.exit(1)
    else:
        cD.start(log_fn)
