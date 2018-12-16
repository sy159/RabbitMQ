# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

from rabbitmq import RabbitPublisher

if __name__ == "__main__":
    # 新建一个队列链接
    rab_con = RabbitPublisher(queuename="fish_test")
    # 模拟进行生产，输入进队列
    for a in range(100):
        body_json = json.dumps({"a": a})
        rab_con.in_queue(body_json)
    # 断开链接
    rab_con.stop()
