# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import time

import requests


# 自定义的回调函数、消费方法。主要用body里的参数
def callbackworker(body):
    print body
    time.sleep(1)
    # r = requests.get(url="http://www.handibigdat3a.com", timeout=1)
    # print r.text
