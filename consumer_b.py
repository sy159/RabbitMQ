# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from callback import callbackworker
from rabbitmq import RabbitConsumerAysnc

# 下面是非守护进程的启动方法
if __name__ == "__main__":
    # 新建一个队列链接
    rab_con = RabbitConsumerAysnc(queuename="fish_test", callbackworker=callbackworker, prefetch_count=2)
    # 开启消费者进程
    rab_con.run()
