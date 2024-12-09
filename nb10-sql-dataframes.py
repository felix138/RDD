#coding:utf8
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import urllib.request


if __name__ == '__main__':
    conf = (
        SparkConf()
        .setAppName("RDD whole files")
        .setMaster("spark://localhost:7077")
        .set("spark.driver.bindAddress", "0.0.0.0")
        .set("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
        .set("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python3")
        .set("spark.executor.memory", "2g")
        .set("spark.executor.cores", "2")
    )
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)
    print("test11111111111111111111111111111111111111111111111111111")

    #f = urllib.request.urlretrieve("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz",
                          # "C:/root/kddcup.data_10_percent.gz")

    # 使用绝对路径并添加 file:// 协议
  #  data_file = os.path.abspath("/mnt/data/kddcup.data_10_percent.gz").replace("\\", "/")
    data_file = "file:///mnt/data/kddcup.data_10_percent.gz"

    print("Data file path:", data_file)
   # data_file = "./kddcup.data_10_percent.gz"
    raw_data = sc.textFile(data_file).cache()
    csv_data = raw_data.map(lambda l: l.split(","))
    row_data = csv_data.map(lambda p: Row(
        duration=int(p[0]),
        protocol_type=p[1],
        service=p[2],
        flag=p[3],
        src_bytes=int(p[4]),
        dst_bytes=int(p[5])
    )
)
    interaction_df = sqlContext.createDataFrame(row_data)
    interaction_df.registerTempTable("interactions")

    tcp_interactions = sqlContext.sql("""
    SELECT duration, dst_bytes FROM interactions WHERE protocol_type = 'tcp' AND duration > 1000 AND dst_bytes = 0
""")
    tcp_interactions.show()