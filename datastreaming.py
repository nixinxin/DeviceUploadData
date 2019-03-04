# -*- coding: utf-8 -*-
import random
import time
import json


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


import happybase


def get_data():
    pool = happybase.ConnectionPool(size=3, host='172.16.1.225')
    with pool.connection() as connection:
        result = connection.table("device_upload_data")
        for key, value in result.scan():
            print(key, value)


def data_format(**data):
    result = {}
    row_key = None
    for key, value in data.items():
        if key == '@timestamp':
            result[str(key) + ":" + str(key)] = str(data[key])
            row_key = value
        elif key == 'request_body':
            value = json.loads(value)[0]
            for i, j in value.items():
                if not isinstance(j, dict):
                    result[str(key) + ":" + str(i)] = str(j)
                else:
                    for e, f in j.items():
                        result[str(key) + ":" + str(e)] = str(f)

        elif key == '@fields':
            for i, j in value.items():
                result[str(key) + ":" + str(i)] = str(j)
    return row_key, result


def insert_hbase(format_data):

    pool = happybase.ConnectionPool(size=3, host='172.16.1.225')
    with pool.connection() as connection:
        table = connection.table("device_upload_data")
        try:
            with table.batch(transaction=True) as b:
                status = b.put(format_data[0] + str(time.time()) + str(random.randint(1, 1000)), format_data[1])
        except Exception as e:
            status = e
    return status


def data_log(str):
    time_format = time.strftime(r"%Y-%m-%d %H:%M:%S", time.localtime())
    return "[%s]%s" % (time_format, str)


# 处理RDD元素，此RDD元素需为字典类型
def fmt_data(msg_dict):
    msg_dict = json.loads(msg_dict)
    data = data_format(**msg_dict)
    status = insert_hbase(data)
    print(status, msg_dict['@timestamp'])
    return msg_dict


def connectAndWrite(data):
    if not data.isEmpty():
        device_upload_data = data.map(lambda x: x[1])
        msg_row = device_upload_data.map(lambda x: fmt_data(x))
        msg_row.collect()


if __name__ == '__main__':

    host = '172.16.2.200'
    zkQuorum = host + ':2181'
    groupId = '172.16.2.200:2181'

    sc = SparkContext("local[2]".format(host), appName="logfile-streaming")
    # sc.setLogLevel("info")
    ssc = StreamingContext(sc, 1)
    kafkaStreams = KafkaUtils.createStream(ssc=ssc,
                                           zkQuorum=zkQuorum,
                                           groupId='0',
                                           topics={"logfile-memory-kafka": 1},
                                           kafkaParams={'logfile-memory-kafka': 1})

    kafkaStreams.foreachRDD(connectAndWrite)

    ssc.start()
    ssc.awaitTermination()


