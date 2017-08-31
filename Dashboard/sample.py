import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple
from pyspark.sql import SQLContext, Row

def aggregateRecord(rdd):
     #data = rdd.take(3)
     if rdd.count() > 0:
         df = rdd.toDF()
         df.show()
     #print data

# Start Spark Context and SQL Context
sc = SparkContext(appName="qoo")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
ssc        = StreamingContext(sc, 1)

lines  = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

fields = ("machine_id", "log_message", "log_value")
mlog   =   namedtuple(  'mlog', fields )

counts = lines.map(lambda rec: rec.split(','))\
              .filter(lambda li: 'error' in li[1])\
              .map(lambda rec: mlog(rec[0], rec[1], rec[2]))


#counts.pprint()
counts.foreachRDD(aggregateRecord)



# Start the computation
ssc.start()
# Wait for the computation to terminate
ssc.awaitTermination()
