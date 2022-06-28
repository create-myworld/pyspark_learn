# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        master("local[*]").\
        appName("hello").\
        getOrCreate()
    sc = spark.sparkContext

    # 构建一个RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7]).map(lambda x: [x])
    df = rdd.toDF(['num'])

    # TODO 1: 方式一 SparkSession.udf.register()
    # UDF处理函数
    def num_ride_10(num):
        return num * 10
    # 参数一：注册的UDF名称，仅用于SQL风格
    # 参数二：需要注册的UDF的处理函数名
    # 参数三：声明UDF返回类型，并且UDF的真实返回值一定要和声明的返回值类型一致
    # 声明的udf对象可用于DSL语法
    udf1 = spark.udf.register("udf1", num_ride_10, IntegerType())
    df.show()
    df.selectExpr("udf1(num)").show()
    df.select(udf1(df['num'])).show()
    # TODO 2: 方式二 pyspark.sql.functions.udf 仅用于DSL风格
    udf2 = F.udf(num_ride_10, IntegerType())
    df.select(udf2(df['num'])).show()
