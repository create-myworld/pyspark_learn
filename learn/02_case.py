import re

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':

    conf = SparkConf().setMaster("local[*]").setAppName("case")
    sc = SparkContext(conf=conf)

    # 1.读取数据文件
    file_rdd = sc.textFile("../data/input/accumulator_broadcast_data")

    # 特殊字符的list定义
    abnormal_char = [",", ".", "!", "#", "$", "%"]

    # 2.特殊字符List 包装成广播变量
    broadcast = sc.broadcast(abnormal_char)

    # 3.对特殊字符出现次数做累加，累加使用累加器
    acmlt = sc.accumulator(0)

    # 4.数据处理，先处理数据中的空白行，在python中有内容就是ture none 就是false
    lines_rdd = file_rdd.filter(lambda line: line.strip())

    # 5.去除前后的空格
    data_rdd = lines_rdd.map(lambda line: line.strip())

    # 6.将数据进行切分，按照正则表达式切分，因为空格分隔符某些单词之间是两个或多个空格
    # 正则表达式 \s+ 表示 不确定多少个空格，最少一个空格

    word_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))

    # 7.当前word_rdd中有正常单词，也有特殊字符
    # 当前需要过滤数据，保留正常单词用于单词计数，再过滤的过程中， 对特殊字符计数
    def filte_func(data):
        """过滤数据，保留正常单词用于单词计数，在过滤的过程中，对特殊字符做计数"""
        global acmlt
        #取出广播变量中存储的特殊符号list
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            # 表示这个是特殊字符
            acmlt += 1
            return False
        else:
            return True

    normal_words_rdd = word_rdd.filter(filte_func)

    # 8.正常单词单词计数逻辑
    result_rdd = normal_words_rdd.map(lambda x: (x, 1)).\
        reduceByKey(lambda a, b: a + b)
    print("正常单词计数结果：", result_rdd.collect())
    print("特殊字符数量：", acmlt)