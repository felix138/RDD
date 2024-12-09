#coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == "__main__":

# Set up the Spark config
   conf =(
       SparkConf()
       .setAppName("test textFile API")
       .setMaster("local[*]")
   )
   sc = SparkContext(conf= conf)
   file_rdd1 = sc.textFile(r"..\data\input\hello.txt")
   print("default file parations number ", file_rdd1.getNumPartitions())
   print("file_rdd1 context", file_rdd1.collect())

   #add Minimum parations test
   file_rdd2 = sc.textFile(r"..\data\input\hello.txt",3)

   #add 100 , spark just using 75, it will control by RDD it self
   file_rdd3 = sc.textFile(r"..\data\input\hello.txt",100)
   print("file_rdd2",file_rdd2.getNumPartitions())
   print("file_rdd2",file_rdd3.getNumPartitions())

   #read HDFS file test
   hdfs_rdd = sc.textFile("hdfs://host.docker.internal:9000/data/openbeer/breweries/breweries.csv")
   print("hdfs data", hdfs_rdd.collect())