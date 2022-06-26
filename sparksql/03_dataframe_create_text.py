# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        master("local[*]").\
        appName("test").\
        getOrCreate()

    # 构建StructType，text数据源，读取数据的特点是，将一整列只作为一列读取，默认列名是v，类型是String
    schema = StructType().add("data", StringType(), nullable=True)
    df = spark.read.format("text").\
        schema(schema=schema).\
        load("../data/input/people")
    df.printSchema()
    df.show()

    # 读取json数据源,json类型自带Schema
    df1 = spark.read.format("json").load("../data/input/people.json")
    df1.printSchema()
    df1.show()

    # 读取csv文件
    df2 = spark.read.format("csv"). \
        option("sep", ","). \
        option("header", False).\
        option("encoding", "utf-8").\
        schema("name STRING, age INT, job STRING").\
        load("../data/input/people")

    df2.printSchema()
    df2.show()

    # 读取parquet文件
    # parquet是spark中常用的一种列式存储文件格式
    # parquet内置schema
    # parquet列式存储
    # parquet序列化
    """spark.read.format("parquet").\
        load("../data/input/")"""

