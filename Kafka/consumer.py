from kafka import KafkaConsumer
import io

# To consume messages
consumer = KafkaConsumer('my-topic',
                         group_id='my_group',
                         bootstrap_servers=['localhost:9092'])


for msg in consumer:
    print msg