# coding:utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*}").\
        getOrCreate()
    sc = spark.sparkContext

    spark.read.format("csv").\
        schema("id INT, subject STRING, score INT").\
        load()

    