# coding:utf8
from pyspark import SparkConf, SparkContext

# 临时环境变量
# import os
# os.environ['PYSPARK_PYTHON'] = "C:\\Users\\Yangzh\\miniconda3\\envs\\pyspark_learn\\python.exe"
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("hello")
    # 提交到集群就不需要指定master了
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)


    # 传统方式
    # count = 0

    # spark提供的累加器变量，参数是初始值
    acmlt = sc.accumulator(0)

    def map_func(data):
        # global 全局变量
        #global count
        #
        # count += 1
        #print(count)

        global acmlt
        acmlt += 1
        print(acmlt)
    result = rdd.map(map_func)

    # 将rdd缓存在内存中
    result.cache()

    # collect 触发job的action算子 原rdd已经不存在了

    result.collect()

    result1 = result.map(lambda x: x)
    result1.collect()

    print(acmlt)

    # 本地化集合是在driver中计算的
    # 分布式中累加器与传统的不一样 需要使用spark中带的累加器进行数据的累加
