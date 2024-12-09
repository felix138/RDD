# coding:utf8
import os
import sys
import subprocess
from pprint import pprint

from pyarrow import duration
from pyspark import SparkConf, SparkContext
from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession, Row
from time import time
import urllib.request

# 清除可能指向 Docker 的环境变量
if 'SPARK_MASTER' in os.environ:
    del os.environ['SPARK_MASTER']

if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']

if 'SPARK_CONF_DIR' in os.environ:
    del os.environ['SPARK_CONF_DIR']
# check exist for alive sparksession

# 设置 Python 解释器路径，确保使用 Conda 环境中的 Python

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
existing_spark = SparkSession.getActiveSession()
existing_spark.stop()

# setup spark config and using local spark
conf = (
    SparkConf()
    .setAppName("RDD whole1 files")
    .setMaster("local[*]")  # 确保使用本地模式，不连接到 Docker
    .set("spark.executor.memory", "14g")  # 设置内存
    .set("spark.driver.memory", "8g")
    .set("spark.network.timeout", "600s")  # 增加网络超时
    .set("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
    .set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", sys.executable)
    .set("spark.executorEnv.PYTHONHASHSEED", "0")  # 保证所有节点使用相同的Python环境
    .set("spark.python.worker.reuse", "true")
)
print(f"Driver Python version: {sys.version}")
worker_version = subprocess.check_output([os.environ.get('PYSPARK_PYTHON'), '--version'],
                                         stderr=subprocess.STDOUT).decode('utf-8').strip()
print(f"Worker Python version: {worker_version}")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext

print("Spark application is running locally")
print(f"Using Python interpreter: {sys.executable}")

local_file_path = "C:/root/kddcup.data_10_percent.gz"
urllib.request.urlretrieve("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", local_file_path)

# 使用 format 方法构造数据文件路径，确保以 file:/// 开头
data_file = "file:///{}".format(local_file_path.replace("\\", "/"))

print("Data file path:", data_file)

raw_data = sc.textFile(data_file)
normal_raw_data = raw_data.filter(lambda x: 'normal.' in x)
normal_count = normal_raw_data.count()
print ("There are {} 'normal' interactions".format(normal_count))

#the map transformation
csv_data = raw_data.map(lambda x: x.split(","))
head_rows = csv_data.take(10)

pprint(head_rows[0])


#predefined functions

def parse_interaction(line):
    elems = line.split(",")
    tag = elems[41]
    return (tag, elems)

key_csv_data = raw_data.map(parse_interaction)

head_rows = key_csv_data.take(5)
pprint(head_rows[0])
sc.stop()
spark.stop()
