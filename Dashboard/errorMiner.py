import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import happybase, uuid, json, time
from collections import namedtuple
from pyspark.sql import SQLContext, Row
from datetime import datetime


def aggregateRecord(rdd):
    print rdd.count()
    connection.open()
    table = connection.table('mytable')
    current_date = datetime.now()
    dt = current_date.timetuple()
    ts = time.mktime(dt)
    if rdd.count() > 0:
        dic = {}
		# Data Aggregate
        df = rdd.toDF()
        df.registerTempTable("mlog")
        agg = sqlContext.sql("SELECT machine_id, avg(log_value) FROM mlog group by machine_id")
        for k, v in agg.collect():
            dic[k] = v
        print dic
		# put data into hbase
        table.put(str(int(ts)), {'cf1:m1' : str(dic.get('1', 0))})
        table.put(str(int(ts)), {'cf1:m2' : str(dic.get('2', 0))})
        table.put(str(int(ts)), {'cf1:m3' : str(dic.get('3', 0))})
        table.put(str(int(ts)), {'cf1:m4' : str(dic.get('4', 0))})
        table.put(str(int(ts)), {'cf1:m5' : str(dic.get('5', 0))})
    connection.close()





# Start Spark Context and SQL Context
sc = SparkContext(appName="qoo")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
ssc        = StreamingContext(sc, 1)

# Read Data from Text Stream
lines  = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

# Process Data
fields = ("machine_id", "log_message", "log_value")
mlog   = namedtuple( 'mlog', fields )
counts = lines.map(lambda line:line.split(','))\
              .filter(lambda ele: 'error' in ele[1])\
              .map( lambda rec: mlog( rec[0], rec[1], int(rec[2])))

# Aggregate and Insert Data			  
connection = happybase.Connection('localhost', autoconnect=False)
counts.foreachRDD(aggregateRecord)

# Start the computation
ssc.start()             
# Wait for the computation to terminate
ssc.awaitTermination()  
