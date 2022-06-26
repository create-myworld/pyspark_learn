# coding:utf8

# SparkSession对象导入，对象来自于pyspark.sql
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象：
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 通过SparkSession对象，获取SparkContext对象
    sc = spark.sparkContext

    # 指定分隔符，第一列为表头与否
    df = spark.read.csv("../data/input/stu_info", sep=',', header=False)
    df1 = df.toDF("id", "name", "score")
    # 打印schema结构
    df1.printSchema()
    # 展示df数据
    # df1.show()

    # 创建临时视图
    df1.createTempView("hello")

    # SQL风格
    spark.sql("""
    select name from hello group by name 
    """).show()

    # DSL风格
    df1.where("name='语文'").limit(3).show()
