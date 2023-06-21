import pandas as pd
import unittest

from main import CustomJoinPipelines


class MyTestCase(unittest.TestCase):
    def setUp(self):

        self.redis_df = pd.read_csv("test_dataset/test_redis_data.csv", index_col=0)
        self.redis_df['user_id'] = self.redis_df['user_id'].astype('int64')
        self.redis_df['timestamp'] = pd.to_datetime(self.redis_df['timestamp'])

        self.sql_df = pd.read_csv("test_dataset/test_sql_data.csv", index_col=0)
        self.sql_df['user_id'] = self.sql_df['user_id'].astype('int64')
        self.sql_df['timestamp'] = pd.to_datetime(self.sql_df['timestamp'])

        self.test_result_df = pd.read_csv("test_dataset/test_result.csv", index_col=0)
        self.test_result_df['timestamp_x'] = pd.to_datetime(self.test_result_df['timestamp_x'])
        self.test_result_df['timestamp_y'] = pd.to_datetime(self.test_result_df['timestamp_y'])
        self.test_result_df['user_id'] = self.test_result_df['user_id'].astype('int64')
        self.test_result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        self.test_result_df.reset_index(drop=True, inplace=True)

        self.pipelines = CustomJoinPipelines()

    def tearDown(self):
        pass

    def test_normal_join(self):
        result_df = self.pipelines.normal_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id')
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_pipelined_hash_join_with_10_partitions(self):
        result_df = self.pipelines.pipelined_hash_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=10)
        result_df = pd.concat(result_df)
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_pipelined_hash_join_with_50_partitions(self):
        result_df = self.pipelines.pipelined_hash_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=50)
        result_df = pd.concat(result_df)
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_pipelined_hash_join_with_100_partitions(self):
        result_df = self.pipelines.pipelined_hash_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=100)
        result_df = pd.concat(result_df)
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_semi_join_with_10_partitions(self):
        result_df = self.pipelines.semi_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=10)
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_semi_join_with_50_partitions(self):
        result_df = self.pipelines.semi_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=50)
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_semi_join_with_100_partitions(self):
        result_df = self.pipelines.semi_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=100)
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(self.test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_intersection_bloom_filter_join_10_partitions(self):
        result_df = self.pipelines.intersection_bloom_filter_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=10)
        result_df["user_id"] = result_df["user_id"].astype('int64')
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        test_result_df = pd.read_csv("test_dataset/test_bloom_filter_result.csv", index_col=0)
        test_result_df['timestamp_x'] = pd.to_datetime(test_result_df['timestamp_x'])
        test_result_df['timestamp_y'] = pd.to_datetime(test_result_df['timestamp_y'])
        test_result_df['user_id'] = test_result_df['user_id'].astype('int64')
        test_result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        test_result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_intersection_bloom_filter_join_50_partitions(self):
        result_df = self.pipelines.intersection_bloom_filter_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=50)
        result_df["user_id"] = result_df["user_id"].astype('int64')
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        test_result_df = pd.read_csv("test_dataset/test_bloom_filter_result.csv", index_col=0)
        test_result_df['timestamp_x'] = pd.to_datetime(test_result_df['timestamp_x'])
        test_result_df['timestamp_y'] = pd.to_datetime(test_result_df['timestamp_y'])
        test_result_df['user_id'] = test_result_df['user_id'].astype('int64')
        test_result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        test_result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(test_result_df.equals(result_df), True, "Dataframes are not equal")

    def test_intersection_bloom_filter_join_100_partitions(self):
        result_df = self.pipelines.intersection_bloom_filter_join(df1=self.redis_df, df2=self.sql_df, join_key='user_id', npartitions=100)
        result_df["user_id"] = result_df["user_id"].astype('int64')
        result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        result_df.reset_index(drop=True, inplace=True)

        test_result_df = pd.read_csv("test_dataset/test_bloom_filter_result.csv", index_col=0)
        test_result_df['timestamp_x'] = pd.to_datetime(test_result_df['timestamp_x'])
        test_result_df['timestamp_y'] = pd.to_datetime(test_result_df['timestamp_y'])
        test_result_df['user_id'] = test_result_df['user_id'].astype('int64')
        test_result_df.sort_values(by=['user_id', 'timestamp_x', 'timestamp_y'], inplace=True)
        test_result_df.reset_index(drop=True, inplace=True)

        self.assertEqual(test_result_df.equals(result_df), True, "Dataframes are not equal")

if __name__ == '__main__':
    unittest.main()
