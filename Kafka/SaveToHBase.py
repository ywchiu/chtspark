from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import happybase, uuid, json
connection = happybase.Connection('localhost', autoconnect=False)

def SaveRecord(rdd):
    connection.open()
    table = connection.table('mytable')
    datamap = rdd.collect()
    for k, v in datamap:
        v2 = json.loads(v)
        #print v2
        table.put(str(uuid.uuid4()), {'cf1:col1': v2['name']})
    connection.close()

sc = SparkContext(appName="qoo")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "GroupNameDoesntMatter", {"queue_test": 1})


messages = kafkaStream.map(lambda xs:xs)
messages.foreachRDD(SaveRecord)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
