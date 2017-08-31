from kafka import KafkaConsumer
import io

# To consume messages
consumer = KafkaConsumer('queue_test',
                         bootstrap_servers=['localhost:9092'])


for msg in consumer:
    print msg