import sqlite3
import unittest

import redis

from main import redis_to_pandas, sql_to_pandas, CustomJoinPipelines


class MyTestCase(unittest.TestCase):
    def setUp(self):
        conn = sqlite3.connect('data1/mydatabase.db')
        cursor = conn.cursor()

        r = redis.Redis(host='localhost', port=6379, decode_responses=True)

        redis_data = r.hgetall('test_dataset')

        self.redis_df = redis_to_pandas(redis_data)
        cursor.execute("SELECT * FROM test_dataset")

        sqlite_data = cursor.fetchall()
        self.sql_df = sql_to_pandas(sqlite_data)

        self.pipelines = CustomJoinPipelines()

    def tearDown(self):
        pass


    def test_1(self):
        result = self.pipelines.pipelined_hash_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=4)
        print(result)

if __name__ == '__main__':
    unittest.main()
