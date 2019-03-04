# -*- coding: utf-8 -*-
# @Time    : 2019/2/27 14:42
# @Author  : Shark
# @File    : 2019-02-27.py
import random
import time
from decimal import Decimal
import argparse

from apscheduler.schedulers.blocking import BlockingScheduler

source_str = (r'{  "@timestamp": "%s",  '
              '"request_body": "[{\\"cmd\\":\\"021001\\",'
              '\\"data\\":{\\"sn\\":3,\\"battery\\":%s,\\"rssi\\":99,'
              '\\"time\\":1551169181989,\\"time_string\\":\\"%s\\",'
              '\\"warn\\":1,\\"object_direction\\":1,\\"object_move\\":false,\\"ntc_temperature\\":%f,'
              '\\"dig_temperature\\":%f,\\"humidity\\":%f,\\"pressure\\":0,\\"x_amplitude\\":%d,'
              '\\"x_frequency\\":%d,\\"y_amplitude\\":%d,\\"y_frequency\\":%d,\\"z_amplitude'
              '\\":%d,\\"z_frequency\\":%d},\\"device_code\\":\\"002108010712211D\\",\\"type\\":'
              '\\"device_data\\",\\"time\\":%s}]",  '
              '"@fields": {    "host": "wechat.v3.api.2012iot.com",    '
              '"server_port": "80",    "remote_addr": "192.168.5.175",    '
              '"upstream_addr": "10.21.10.79:8080",    '
              '"remote_user": "",    "request": "POST /api-wechat/alarm/alarmServer HTTP/1.0",    '
              '"status": "200",    '
              '"upstream_status": "200",    '
              '"request_time": "0.472",    '
              '"upstream_response_time": "0.472",    '
              '"body_bytes_sent": "12",    '
              '"http_referer": "",    '
              '"http_user_agent": "",    '
              '"http_x_forwarded_for": "47.100.234.175, 192.168.5.152",   '
              ' "http_tokenStr": ""  }}')


def random_float():
    a = random.uniform(1, 30)
    return Decimal(str(a)).quantize(Decimal('0.00'))


def random_int():
    a = random.randint(1000, 10000)
    return a


def time_stamp(time_str):
    return int(round(time_str * 1000))


def random_battery():
    a = random.randint(1, 100)
    return a


def str_time(time_str):
    t = time.strftime('%d/%b/%Y:%H:%M:%S +0800', time.localtime(time_str))
    return t


def str_time2(time_str):
    t = time.strftime('%d/%m/%Y:%H:%M:%S', time.localtime(time_str))
    return t


def main():

    n = 0
    f = open(args.file, 'a', encoding='utf8')
    while n < int(args.number):
        w = (source_str % (str_time(t), random_battery(), str_time2(t), random_float(), random_float(), random_float(),
                           random_int(), random_int(), random_int(), random_int(), random_int(), random_int(),
                           time_stamp(t)))
        f.write(w + '\n')
        n += 1
    f.close()


if __name__ == '__main__':
    schedule = BlockingScheduler()
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', dest='file', help='文件名')
    parser.add_argument('-t', '--time', dest='time', help='多长时间发送')
    parser.add_argument('-n', '--number', dest='number', help='一次写入几条')
    args = parser.parse_args()
    t = time.time()
    schedule.add_job(func=main, trigger='interval', seconds=int(args.time), id='job1')
    schedule.start()

