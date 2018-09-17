from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import happybase, uuid, json
connection = happybase.Connection('localhost', autoconnect=False)

def SaveRecord(rdd):
    connection.open()
    table   = connection.table('system_memory')
    datamap = rdd.collect()

    for k, v in datamap:
        jd = json.loads(v)
        table.put(str(jd['tm']), {'system_usage:memory': str(jd['memory'])})
    connection.close()

sc  = SparkContext(appName="qoo")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "GroupNameDoesntMatter", {"system_monitor": 1})


messages = kafkaStream.map(lambda xs:xs)
messages.foreachRDD(SaveRecord)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
