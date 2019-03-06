# -*- coding: utf-8 -*-
import random
import time
import json


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


import happybase


a = 0


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


def connect_and_write(rows):
    pool = happybase.ConnectionPool(size=2, host='172.16.1.225')
    with pool.connection() as connection:
        table = connection.table("device_upload_data")
        try:
            with table.batch(batch_size=100) as b:
                for row in rows:
                    msg_dict = json.loads(row[1])
                    row = data_format(**msg_dict)
                    b.put(row[0] + str(time.time()) + str(random.randint(1, 1000)), row[1])
        except Exception as e:
            print(e)


if __name__ == '__main__':

    host = '172.16.1.225'
    zkQuorum = host + ':2181'
    groupId = '172.16.2.200:2181'

    sc = SparkContext("local[2]".format(host), appName="logfile-streaming")
    sc.setLogLevel("info")
    ssc = StreamingContext(sc, 1)
    kafkaStreams = KafkaUtils.createStream(ssc=ssc,
                                           zkQuorum=zkQuorum,
                                           groupId='0',
                                           topics={"logfile-memory-kafka": 1},
                                           kafkaParams={'logfile-memory-kafka': 1})

    kafkaStreams.foreachRDD(lambda rdd: rdd.foreachPartition(connect_and_write))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


