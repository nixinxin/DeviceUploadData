import happybase


class ToHbase(object):

    def __init__(self, host, size=3, autoconnect=True):
        self.host = host
        self.size = size
        self.autoconnect = autoconnect

    def connect(self):
        pool = happybase.ConnectionPool(size=self.size, host=self.host, autoconnect=self.autoconnect)

        return pool

    def insert(self):
        pool = self.connect()
        with pool.connection() as connection:
            connection.open()
            table = connection.tables()
            print(table)


if __name__ == '__main__':

    connect = ToHbase(host='127.0.0.1')
    connect.insert()

