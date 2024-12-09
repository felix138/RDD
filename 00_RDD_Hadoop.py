import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'


from pyspark import SparkConf,SparkContext


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
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("hdfs://host.docker.internal:9000/data/openbeer/breweries/breweries.csv")

    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    print(words_rdd.collect())