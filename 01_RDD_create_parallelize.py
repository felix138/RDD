#conding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == '__main__':

   #init env
   conf =(
       SparkConf()
       .setAppName("test")
       .setMaster("spark://localhost:7077")
       .set("spark.driver.bindAddress", "0.0.0.0")
       .set("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
       .set("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python3")
       .set("spark.executor.memory", "2g")
       .set("spark.executor.cores", "2")
   )

   sc = SparkContext(conf=conf)
   rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])
   #parallelize no numslices, Local list to RDD list
   print("Print num partitions : ", rdd.getNumPartitions())

   # parallelize set numslices
   rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],16)
   print("Print num partitions : ", rdd.getNumPartitions())

   #The data on the distributed nodes are unified on the driver to form a Python list
   print("rdd context:", rdd.collect())

