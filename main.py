# This is a sample Python script.
# coding:utf8
from pyspark import SparkConf, SparkContext


#临时环境变量
# import os
# os.environ['PYSPARK_PYTHON'] = "C:\\Users\\Yangzh\\miniconda3\\envs\\pyspark_learn\\python.exe"
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("hello")
    # 提交到集群就不需要指定master了

    sc = SparkContext(conf=conf)

    data = sc.textFile(r"D:\IdeaProjects\Spark_learn\src\data\wordcount.data")
    # data = sc.textFile("/data/data")
    word_rdd = data.flatMap(lambda line: line.split(" "))
    word_map_rdd = word_rdd.map(lambda x: (x, 1))
    result = word_map_rdd.reduceByKey(lambda a, b: a+b).collect()

    print(result)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
