# -*- coding: utf-8 -*-
import time, json, random
from kafka import KafkaConsumer, KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092')
i = 0
while True:
    dic = {}
    dic['id'] = i
    dic['name'] = 'qoo' + str(i)
    dic['value'] = random.random()
    d = json.dumps(dic)
    i = i + 1

    producer.send('queue_test',d)
    time.sleep(1)
    print "awake", i
