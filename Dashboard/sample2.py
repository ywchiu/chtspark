import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple
from pyspark.sql import SQLContext, Row
from datetime import datetime
import happybase
import uuid, time
connection = happybase.Connection('localhost', autoconnect=False)


def aggregateRecord(rdd):
     connection.open()
     table = connection.table('mytable')
     #data = rdd.take(3)
     current_date = datetime.now()
     dt = current_date.timetuple()
     ts = time.mktime(dt)
     if rdd.count() > 0:
         df = rdd.toDF()
         #df.show()
         df.registerTempTable('mlog')
         agg = sqlContext.sql('select machine_id, avg(log_value) from mlog group by machine_id')

         dic = {}
         for k, v in agg.collect():
             dic[k] = v
         table.put(str(int(ts)),{'cf1:m1' : str(dic.get('1', 0))})
         table.put(str(int(ts)),{'cf1:m2' : str(dic.get('2', 0))})
         table.put(str(int(ts)),{'cf1:m3' : str(dic.get('3', 0))})
         table.put(str(int(ts)),{'cf1:m4' : str(dic.get('4', 0))})
         table.put(str(int(ts)),{'cf1:m5' : str(dic.get('5', 0))})
     #print data
     connection.close()

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
