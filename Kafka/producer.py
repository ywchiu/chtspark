# -*- coding: utf-8 -*-
import time, json
from kafka import KafkaConsumer, KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092')
i = 0
while True:
    dic = {}
    dic['id'] = i
    dic['name'] = 'qoo' + str(i)
    d = json.dumps(dic)
    i = i + 1

    producer.send('queue_test',d)
    #producer.send('queue_test',"mundo!")
    time.sleep(1)
    print "awake"
