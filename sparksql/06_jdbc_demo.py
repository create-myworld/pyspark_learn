# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
import pandas as pd


if __name__ == '__main__':
    spark = SparkSession.builder.\
        master("local[*]").\
        appName("test").\
        getOrCreate()

    schema = StructType().add("user_id", StringType(), nullable=True).\
        add("movie_id", IntegerType(), nullable=True).\
        add("rank", IntegerType(), nullable=True).\
        add("ts", StringType(), nullable=True)

    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("../data/input/u.data")

    """ df.write.mode("overwrite").\
        format("jdbc").\
        option("url", "jdbc:mysql://localhost:3306/bigdata?useSSL=false&useUnicode=true").\
        option("dbtable", "bigdata").\
        option("user", "root").\
        option("password", "123456").\
        save() """

    """
    JDBC,自动创建表格
    DF中有表结构信息，StructType记录各个字段的 名称 类型 和是否为空
    """

    spark.read.format("jdbc").\
        option("url", "jdbc:mysql://localhost:3306/bigdata?useSSL=false&useUnicode=true"). \
        option("dbtable", "bigdata"). \
        option("user", "root"). \
        option("password", "123456").\
        load().show()