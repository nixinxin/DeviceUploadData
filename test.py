import json

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
            with table.batch(batch_size=10) as b:
                status = b.put(format_data[0], format_data[1])
        except Exception as e:
            status = e
    return status


if __name__ == '__main__':
    # data = {'@timestamp': '04/Mar/2019:13:05:12 +0800', 'request_body': '[{"cmd":"021001","data":{"sn":3,"battery":53,"rssi":99,"time":1551169181989,"time_string":"04/03/2019:13:05:12","warn":1,"object_direction":1,"object_move":false,"ntc_temperature":10.210000,"dig_temperature":10.100000,"humidity":10.680000,"pressure":0,"x_amplitude":6156,"x_frequency":3516,"y_amplitude":3938,"y_frequency":7213,"z_amplitude":4781,"z_frequency":1297},"device_code":"002108010712211D","type":"device_data","time":1551675912463}]', '@fields': {'host': 'wechat.v3.api.2012iot.com', 'server_port': '80', 'remote_addr': '192.168.5.175', 'upstream_addr': '10.21.10.79:8080', 'remote_user': '', 'request': 'POST /api-wechat/alarm/alarmServer HTTP/1.0', 'status': '200', 'upstream_status': '200', 'request_time': '0.472', 'upstream_response_time': '0.472', 'body_bytes_sent': '12', 'http_referer': '', 'http_user_agent': '', 'http_x_forwarded_for': '47.100.234.175, 192.168.5.152', 'http_tokenStr': ''}}
    # data = data_format(**data)
    # status = insert_hbase(data)
    # print(status)
    get_data()