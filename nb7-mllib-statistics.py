# coding:utf8
import os
import sys
import subprocess
from pprint import pprint

import numpy as np
from pyarrow import duration
from pyspark import SparkConf, SparkContext
from pyspark.mllib.stat import Statistics
from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession, Row
from time import time
import urllib.request

from math import sqrt
import pandas as pd

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
#sc.stop()
#spark.stop()
print("Spark application is running locally")
print(f"Using Python interpreter: {sys.executable}")

# download the test data to local
local_file_path = "C:/root/kddcup.data_10_percent.gz"
urllib.request.urlretrieve("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", local_file_path)

data_file = "file:///{}".format(local_file_path.replace("\\", "/"))

raw_data = sc.textFile(data_file)

print("raw_data", raw_data.take(10))

# split data and Remove the data at index 1, 2, 3, 4, 5 and forward the data to float and transfer to Numpy
def parse_interaction(line):
    line_split = line.split(",")
    symbolic_indexes = [1, 2, 3, 41]
    clean_line_split = [item for i, item in enumerate(line_split) if i not in symbolic_indexes]
    #replace normal.
    clean_line_split = [1 if x == 'normal.' else x for x in clean_line_split]
    return np.array([float(x) for x in clean_line_split])


vector_data = raw_data.map(parse_interaction)

#print("vector_data :" , vector_data.take(5))
# Compute column summary statistics.
summary = Statistics.colStats(vector_data)
print("Duration Statistics:")
print("Means:{}", format(round(summary.mean()[0], 3)))
print(" St. deviation: {}".format(round(sqrt(summary.variance()[0]), 3)))
print(" Max value: {}".format(round(summary.max()[0], 3)))
print(" Min value: {}".format(round(summary.min()[0], 3)))
print(" Total value count: {}".format(summary.count()))
print(" Number of non-zero values: {}".format(summary.numNonzeros()[0]))

def parse_interaction_with_key(line):
    line_split = line.split(",")
    #keep just numeric and logical values
    symbolic_indexes = [1,2,3,41]
    clean_line_split = [item for i ,item in enumerate(line_split) if i not in symbolic_indexes]
    return(line_split[41],np.array([float(x) for x in clean_line_split]))

label_vector_data = raw_data.map(parse_interaction_with_key)
# filter on the RDD to leave out other labels but the one we want to gather statistics from

normal_label_data = label_vector_data.filter(lambda x: x[0] == "normal.")
normal_summary = Statistics.colStats(normal_label_data.values())
print ("Duration Statistics for label: {}".format("normal"))
print (" Mean: {}".format(normal_summary.mean()[0],3))
print (" St. deviation: {}".format(round(sqrt(normal_summary.variance()[0]),3)))
print (" Max value: {}".format(round(normal_summary.max()[0],3)))
print (" Min value: {}".format(round(normal_summary.min()[0],3)))
print (" Total value count: {}".format(normal_summary.count()))
print (" Number of non-zero values: {}".format(normal_summary.numNonzeros()[0]))


#Statistics by different labels
def summary_by_label (raw_data,label):
    label_vector_data = raw_data.map(parse_interaction_with_key).filter(lambda x:x[0] == label)
    return Statistics.colStats(label_vector_data.values())

label_list = ["back.", "buffer_overflow.", "ftp_write.", "guess_passwd.",
              "imap.", "ipsweep.", "land.", "loadmodule.", "multihop.",
              "neptune.", "nmap.", "normal.", "perl.", "phf.", "pod.", "portsweep.",
              "rootkit.", "satan.", "smurf.", "spy.", "teardrop.", "warezclient.",
              "warezmaster."]
stats_by_label = [(label,summary_by_label(raw_data,label)) for label in label_list]
duration_by_label = [
    (stat[0], np.array([float(stat[1].mean()[0]),
                        float(sqrt(stat[1].variance()[0])),
                        float(stat[1].min()[0]),
                        float(stat[1].max()[0]),
                        int(stat[1].count())]))
    for stat in stats_by_label]
labels = [stat[0] for stat in duration_by_label]  # 提取标签，顺序与统计数据一致
stats = [stat[1] for stat in duration_by_label]  # 提取统计数据，顺序与标签一致


stats_by_label_df = pd.DataFrame(stats, columns=["Mean", "Std Dev", "Min", "Max", "Count"])
stats_by_label_df.insert(0, "Label", labels)

print ("Duration statistics, by label",stats_by_label_df)



def get_variable_stats_df(stats_by_label, column_i):
    column_stats_by_label = [(stat[0], np.array([float(stat[1].mean()[column_i]),
                        float(sqrt(stat[1].variance()[column_i])),
                        float(stat[1].min()[column_i]),
                        float(stat[1].max()[column_i]),
                        int(stat[1].count())]))
    for stat in stats_by_label
    ]
    labels = [stat[0] for stat in column_stats_by_label]  # 提取标签，顺序与统计数据一致
    stats = [stat[1] for stat in column_stats_by_label]  # 提取统计数据，顺序与标签一致
    stats_by_label_df = pd.DataFrame(stats, columns=["Mean", "Std Dev", "Min", "Max", "Count"])
    stats_by_label_df.insert(0, "Label", labels)
    return stats_by_label_df


get_variable_stats_df(stats_by_label_df,1)
sc.stop()
spark.stop()
