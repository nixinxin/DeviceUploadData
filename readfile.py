from pyspark import SparkConf
from pyspark import SparkContext


if __name__ == '__main__':

    conf = SparkConf().setMaster('local[2]').setAppName("Readfile")

    sc = SparkContext(conf=conf)
    data = sc.textFile('file:///root/apps/projects/data.json')

    data = data.collect()
    print(data)

    sc.stop()