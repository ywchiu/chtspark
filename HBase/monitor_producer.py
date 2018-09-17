# -*- coding: utf-8 -*-
import time, json
from datetime import datetime
import os
import psutil
from kafka import KafkaConsumer, KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')


while True:
    dic = {}
    dic['tm'] = int(time.mktime(datetime.now().timetuple()))
    dic['memory'] = psutil.virtual_memory().percent
    d = json.dumps(dic)

    producer.send('system_monitor',d)
    time.sleep(1)
    print dic