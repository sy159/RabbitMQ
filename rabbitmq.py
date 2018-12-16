# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import threading
import time

import pika

rabbit_host = "192.168.188.128"
rabbit_user = "funntlong"
rabbit_pw = "longfish"


# 同步消息推送类
class RabbitPublisher(object):
    def __init__(self, host=rabbit_host, user=rabbit_user, password=rabbit_pw, queuename="fish_test"):
        self.host = host
        self.user = user
        self.password = password
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=pika.PlainCredentials(self.user, self.password)))
        self.channel = self.connection.channel()
        self.QUEUE = queuename

    def purge(self, queue):
        self.channel.queue_purge(queue)

    def delete(self, queue, if_unused=False, if_empty=False):
        self.channel.queue_delete(queue, if_unused=if_unused, if_empty=if_empty)

    def stop(self):
        self.connection.close()

    def in_queue(self, body, priority=0):
        # 发送消息到队列
        self.channel.queue_declare(queue=self.QUEUE, durable=True)
        self.channel.basic_publish(exchange='', routing_key=self.QUEUE, body=body,
                                   properties=pika.BasicProperties(delivery_mode=2, priority=priority))


class Heartbeat(threading.Thread):
    def __init__(self, connection):
        super(Heartbeat, self).__init__()
        self.lock = threading.Lock()
        self.connection = connection
        self.quitflag = False
        self.stopflag = True
        self.setDaemon(True)

    def run(self):
        while not self.quitflag:
            time.sleep(10)
            self.lock.acquire()
            if self.stopflag:
                self.lock.release()
                continue
            try:
                self.connection.process_data_events()
            except Exception as ex:
                print "Error format: %s" % (str(ex))
                self.lock.release()
                return
            self.lock.release()

    def startheartbeat(self):
        self.lock.acquire()
        if self.quitflag:
            self.lock.release()
            return
        self.stopflag = False
        self.lock.release()


# 同步消息消费类
class RabbitConsumer(object):
    def __init__(self, host=rabbit_host, user=rabbit_user, password=rabbit_pw, queuename="fish_test"):
        self.host = host
        self.user = user
        self.password = password
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=pika.PlainCredentials(self.user, self.password)))
        self.channel = self.connection.channel()
        self.QUEUE = queuename

    def purge(self, queue):
        self.channel.queue_purge(queue)

    def delete(self, queue, if_unused=False, if_empty=False):
        self.channel.queue_delete(queue, if_unused=if_unused, if_empty=if_empty)

    def stop(self):
        self.connection.close()

    def listen_queue(self, queue, callbackworker):
        # 进行消费
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(callbackworker, queue=queue, no_ack=False)
        heartbeat = Heartbeat(self.connection)
        heartbeat.start()  # 开启心跳线程
        heartbeat.startheartbeat()
        self.channel.start_consuming()


# 异步消息消费类
class RabbitConsumerAysnc(object):
    EXCHANGE = 'amq.direct'
    EXCHANGE_TYPE = 'direct'

    def __init__(self, host=rabbit_host, user=rabbit_user, password=rabbit_pw, queuename="fish_test", callbackworker=None, prefetch_count=1):
        self.host = host
        self.user = user
        self.password = password
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.QUEUE = queuename
        self.callbackworker = callbackworker
        self.prefetch_count = prefetch_count

    def connect(self):
        return pika.SelectConnection(pika.ConnectionParameters(host=self.host, credentials=pika.PlainCredentials(self.user, self.password)), self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.basic_qos(prefetch_count=self.prefetch_count)
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        print reply_text
        self._connection.close()

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.EXCHANGE_TYPE, durable=True)

    def on_exchange_declareok(self, unused_frame):
        self.setup_queue()

    def setup_queue(self):
        self._channel.queue_declare(self.on_queue_declareok, self.QUEUE, durable=True)

    def on_queue_declareok(self, method_frame):
        self._channel.queue_bind(self.on_bindok, self.QUEUE, self.EXCHANGE, self.QUEUE)

    def on_bindok(self, unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.QUEUE)

    def add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        self.callbackworker(body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.close_channel()

    def close_channel(self):
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()

    def close_connection(self):
        self._connection.close()
