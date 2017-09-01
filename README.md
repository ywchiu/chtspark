# Apache Spark Streaming巨量資料串流分析實務班




## 下載 Pietty

- https://sites.google.com/view/pietty-project/download?authuser=0




## Spark 範例


- https://raw.githubusercontent.com/ywchiu/micronbigdata/master/Spark/20160704Demo.json

- https://raw.githubusercontent.com/ywchiu/micronbigdata/master/Spark/Spark%20SQL.json



## Kafka
- 下載 kafka: https://pypi.python.org/packages/d2/9b/867bc12ccdfcce600fb6a3b12cef9af30103b2b0c251c6c495e6d6347cae/kafka_python-1.3.4-py2.py3-none-any.whl#md5=96188183cef574393680a17ade3c6bb0

- 透過Ambari 上載至 HDFS /tmp

- hadoop fs -get /tmp/kafka_python-1.3.4-py2.py3-none-any.whl

- pip install kafka_python-1.3.4-py2.py3-none-any.whl


## 使用kafka & Spark Streaming Step1
- spark-submit --driver-java-options "-Dhttp.proxyHost=10.160.3.88 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.160.3.88 -Dhttps.proxyPort=8080" --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.0 consumeFromKafka.py

- spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.0 consumeFromKafka.py


## 安裝 HappyBase
-  export http_proxy=http://10.160.3.88:8080
-  export https_proxy=http://10.160.3.88:8080
- pip install happybase
## HBase 投影片與範例


- https://github.com/ywchiu/micronbigdata/blob/master/HBase/20160707%20-%20HBase%20%E7%B0%A1%E4%BB%8B%E8%88%87%E5%AF%A6%E4%BD%9C.pdf



## Hive Batch 與Spark Streaming 簡介


- https://github.com/ywchiu/micronbigdata/blob/master/Slides/20161201%20%E5%B7%A8%E9%87%8F%E8%B3%87%E6%96%99%E5%AF%A6%E6%88%B0%E8%AA%B2%E7%A8%8B(I)%20%E2%80%93%20Hive%20Batch%20%E8%88%87Spark%20Streaming%20%E7%B0%A1%E4%BB%8B.pdf



## Kafka 與 HBase 簡介


- https://github.com/ywchiu/micronbigdata/blob/master/Slides/20161208%20%E5%B7%A8%E9%87%8F%E8%B3%87%E6%96%99%E5%AF%A6%E6%88%B0%E8%AA%B2%E7%A8%8B(II)%20%E2%80%93%20Kafka%20%E8%88%87%20HBase%20%E7%B0%A1%E4%BB%8B.pdf



## 建立即時分析儀表板


- https://github.com/ywchiu/micronbigdata/blob/master/Slides/20161209%20%E5%B7%A8%E9%87%8F%E8%B3%87%E6%96%99%E5%AF%A6%E6%88%B0%E8%AA%B2%E7%A8%8B(III)%20%E2%80%93%20%E5%BB%BA%E7%AB%8B%E5%8D%B3%E6%99%82%E5%88%86%E6%9E%90%E5%84%80%E8%A1%A8%E6%9D%BF.pdf



## Recommendation System

- https://youtu.be/RoRixPm4XyQ

- https://www.youtube.com/watch?v=l0TemN7lpCs