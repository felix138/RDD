# coding:utf8
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = (
        SparkConf()
        .setAppName("RDD Write to Files")
        .setMaster("spark://localhost:7077")
        .set("spark.driver.bindAddress", "0.0.0.0")
        .set("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
        .set("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python3")
        .set("spark.executor.memory", "2g")
        .set("spark.executor.cores", "2")
    )
    sc = SparkContext(conf=conf)

    # 创建一个简单的 RDD
    data = [
        ("file1.txt", "This is the content of file 1"),
        ("file2.txt", "This is the content of file 2"),
        ("file3.txt", "This is the content of file 3")
    ]

    rdd = sc.parallelize(data)


    # 使用 map 将 RDD 转换为适合写入的格式
    def format_for_save(pair):
        filename, content = pair
        return f"{filename}\n{content}"


    formatted_rdd = rdd.map(format_for_save)

    # 指定输出目录（每个 Executor 会在这个目录下创建文件）
    output_dir = "file:///root/output_files"

    # 保存 RDD 到指定目录
    formatted_rdd.saveAsTextFile(output_dir)

    print(f"Data written to {output_dir}")

    filetext_rdd = sc.wholeTextFiles(output_dir)

    print(filetext_rdd.collect())
