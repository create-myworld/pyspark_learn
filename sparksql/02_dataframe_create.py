# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':
    # 0.构建执行环境入口，
    spark = SparkSession.builder.\
        master("local[*]").\
        appName("hello").\
        getOrCreate()
    sc = spark.sparkContext

    # 1.基于RDD转换成DF
    rdd = sc.textFile("../data/input/people").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))

    # 2.构建DF对象
    # 参数一：被转换的RDD
    # 参数二：指定列名通过list形式指定
    df = spark.createDataFrame(rdd, schema=['name', 'age'])

    # 打印DF的表结构
    df.printSchema()

    # 打印DF中的数据
    # 参数一：展示数据的条数，默认20
    # 参数二：表示是否对列进行截断，如果列的数据长度超过20个，后续就不显示
    # 参数二默认TRUE，设置为FALSE表示全部显示
    df.show(2, False)

    # 将DF对象转换成临时视图，可供sql查询
    df.createTempView("hello")

    spark.sql("""
    select * from hello where age < 20
    """).show()


    # 构建表结构的描述对象：StructType
    # nullable=False 不允许为空
    schema = StructType().add("name", StringType(), nullable=True).\
        add("age", IntegerType(), nullable=False)

    # 基于StrucType对象去构建RDD到DF的转换
    df1 = spark.createDataFrame(rdd, schema=schema)
    df1.printSchema()
    df1.show()

    # RDD toDF方法创建DF
    # 此种方法只有列名，没有列的数据类型
    # 数据类型靠推断，一般是String类型，适用于对数据类型不敏感的数据
    df2 = rdd.toDF(["name", "age"])
    df2.printSchema()
    df2.show()

    # toDF方式二
    df3 = rdd.toDF(schema=schema)
    df3.printSchema()
    df3.show()

    # 基于pandas的DF构建SparkSQL的DF对象
    pdf = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["张大仙", "王笑笑", "张三丰"],
            "age": [18, 21, 25]
        }
    )

    df4 = spark.createDataFrame(pdf)
    df4.printSchema()
    df4.show()
