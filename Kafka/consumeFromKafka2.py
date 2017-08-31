from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

sc = SparkContext(appName="qoo")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)

kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "GroupNameDoesntMatter", {"queue_test": 1})


messages = kafkaStream.map(lambda xs: json.loads(xs[1])['value'] ).\
                reduce(lambda a, b: a+b)

messages.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate