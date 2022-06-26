#encoding=utf8
from pyspark import SparkContext, SparkConf
import os
os.environ['PYSPARK_PYTHON'] = "C:\\Users\\Yangzh\\miniconda3\\envs\\pyspark_learn\\python.exe"
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("hello")
    sc = SparkContext(conf=conf)


    stu_info_list = [(1,'张大仙',11),
                     (2,'王甜甜',13),
                     (3,'张甜甜',11),
                     (4,'王大力',11)]
    # 1、声明广播变量
    broadcast = sc.broadcast(stu_info_list)

    score_info_rdd = sc.parallelize([
        (1,'语文',99),
        (2,'数学',99),
        (3,'英语',99),
        (4,'编程',99),
        (1,'语文',99),
        (2,'编程',99),
        (3,'语文',99),
        (4,'英语',99),
        (1,'语文',99),
        (2,'英语',99),
        (3,'编程',99)
    ])

    def map_fuc(data):
        id = data[0]
        name = ""
        # 匹配本地list和分布式rdd中的学生id 匹配成功后 即可获得当前学生的姓名
        # 2、在使用本地集合对象时，从广播变量中取出来用就行了
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]
        return (name,data[1],data[2])


    print(score_info_rdd.map(lambda x: map_fuc(x)).collect())

"""
场景：本地集合对象和分布式集合对象（RDD）进行关联的时候
需要将本地集合对象 封装为广播变量
可以节省：
1、网络IO的次数
2、Executor的内存占用

备注：如果将本地集合也转换成rdd是不是就可以使用广播变量了呢？
rdd是分布式的，网络IO不受影响了，但是还是会影响executor的性能
而且两个rdd进行计算会产生shuffle更加影响 性能
"""