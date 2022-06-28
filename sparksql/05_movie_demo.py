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

    # 创建临时视图
    df.createTempView("movie")
    # 将临时视图添加进缓存 相当于将rdd持久化在内存中，不然每次使用都要从头构建 浪费资源
    spark.table("movie").cache()

    # TODO 1: 用户平均分
    # SQL 方式
    spark.sql("""
    select user_id,round(avg(rank),2) avg_rank from movie group by user_id order by avg_rank desc limit 5 
    """).show()
    # DSL 方式
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show()

    # TODO 2: 电影的平均分
    # SQL 方式
    spark.sql("""
    select movie_id,count(*) cnt from movie group by movie_id order by cnt desc limit 5
    """).show()
    # DSL 方式
    df.groupBy("movie_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show()

    # TODO 3: 查询大于平均分电影的数量
    # SQL 方式
    spark.sql("""
    select count(distinct movie_id) from movie where rank > (
    select avg(rank) avg_rank from movie ) 
    """).show()
    # DSL 方式
    # 首先查出平均分
    print('看看结果长什么样子：')
    print(df.select(F.avg(df["rank"])).first()['avg(rank)'])
    print("大于平均分电影的数量", df.where(df['rank'] > df.select(F.avg(df["rank"])).first()['avg(rank)']).select("movie_id").distinct().count())

    df1 = df.where(df['rank'] > df.select(F.avg(df["rank"])).first()['avg(rank)'])

    """
    select movie_id from movies group by movie_id 
    """
    print("大于平均分电影的去重数量", df1.select(F.countDistinct("movie_id").alias("cnt")).first()["cnt"])

    # TODO 4: 查询高分电影中(>3)打分次数最多的用户，并求出此人打的平均分
    # SQL 方式
    user_id_max_sql = spark.sql("""
        select user_id,count(*) cnt from movie where rank > 3 group by user_id order by cnt desc
        """).first()['user_id']
    print('打分次数最多的用户是：', user_id_max_sql)
    avg_rank_max_sql = spark.sql(f"""
    select avg(rank) avg_rank from movie where user_id = {user_id_max_sql}
    """).first()['avg_rank']
    print('打分次数最多用户的平均打分是：：', avg_rank_max_sql)
    # DSL 方式
    user_id_max = df.where(df['rank'] > 3).\
        groupBy('user_id').\
        count().\
        orderBy('count', ascending=False).\
        first()['user_id']
    print('打分次数最多的用户是', user_id_max)
    avg_rank_max = df.where(df['user_id'] == user_id_max).\
        groupBy('user_id').\
        avg('rank').\
        first()['avg(rank)']
    print('打分次数最多用户的平均打分是', avg_rank_max)

    # TODO 5：查询每个用户的平均打分，最低打分，最高打分
    # SQL 方式
    spark.sql("""
        select user_id,round(avg(rank),2) avg_rank,min(rank) min_rank,max(rank) max_rank from movie group by user_id
        """)
    # DSL 方式
    df.groupBy('movie_id').\
        agg(
        F.avg('rank').alias('avg_rank'),
        F.min('rank').alias('min_rank'),
        F.max('rank').alias('max_rank')
    )

    # TODO 6：查询被评分超过100次的电影的平均分排名top100
    # SQL 方式
    spark.sql("""
        select movie_id,round(avg(rank),2) avg_rank 
        from movie 
        where movie_id in (
        select movie_id from movie group by movie_id having count(*) > 100) 
        group by movie_id
        order by avg_rank desc  
        """).show()
    # DSL 方式
    df.groupBy("movie_id").\
        agg(
        F.count('movie_id').alias('cnt'),
        F.avg('rank').alias('avg_rank')
    ).where("cnt > 100").\
        orderBy('avg_rank', ascending=False).\
        show()