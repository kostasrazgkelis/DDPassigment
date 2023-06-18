import sqlite3
import time
from datetime import datetime

import redis
import pandas as pd
import ast
import dask.dataframe as dd
from dask import delayed
import cProfile

from dask.tests.test_system import psutil
from dateutil.relativedelta import relativedelta


# Function to calculate the optimal number of partitions
def calculate_partitions():
    # Get the available system resources
    cpu_cores = psutil.cpu_count(logical=False)
    memory = psutil.virtual_memory().total

    # Get the size of your dataset (replace with your actual dataset size)
    dataset_size = 1000000  # Example dataset size

    # Calculate the desired partition size based on system resources
    partition_size = 100000  # Example desired partition size

    # Calculate the optimal number of partitions
    num_partitions = min(cpu_cores, max(1, dataset_size // partition_size))

    return num_partitions


def udf_reformat_to_iso(string: str):
    splits = string.replace(' ', '').split(',')

    if len(splits) < 6:
        splits += ['00' for _ in range(0, 6 - len(splits))]

    year, month, day, hour, minute, second = splits[0], splits[1], splits[2], splits[3], splits[4], splits[5]

    if len(month) != 2:
        month = '0' + month

    if len(day) != 2:
        day = '0' + day

    if len(hour) != 2:
        hour = '0' + hour

    if len(minute) != 2:
        minute = '0' + minute

    if len(second) != 2:
        second = '0' + second

    return f"{year}-{month}-{day}T{hour}:{minute}:{second}"


def redis_to_pandas(data) -> pd.DataFrame:
    df = pd.DataFrame().from_dict(data, orient="index", columns=['raw_data'])
    df.sort_index(inplace=True)
    index_df = df.index

    # Convert the string to a dictionary while preserving the datetime object
    df = pd.DataFrame(df["raw_data"].apply(
        lambda x: ast.literal_eval(x.replace('datetime.datetime', '').replace("(", '"').replace(")", '"'))).tolist())
    df.index = index_df

    df["timestamp"] = df["timestamp"].apply(udf_reformat_to_iso)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.reset_index(drop=False, inplace=True, names='counter')
    return df


def redis_to_pandas(data) -> pd.DataFrame:
    index_df = list(data.keys())
    values = [ast.literal_eval(x.replace('datetime.datetime', '').replace("(", '"').replace(")", '"'))
              for x in data.values()]

    df = pd.DataFrame(values, index=index_df)
    df["timestamp"] = df["timestamp"].apply(udf_reformat_to_iso)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.reset_index(drop=False, inplace=True, names='counter')

    return df


def sql_to_pandas(data) -> pd.DataFrame:
    df = pd.DataFrame(data, columns=['counter', 'user_id', 'timestamp'])
    df['user_id'] = df['user_id'].astype('int64')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def assign_partitions(df, join_key, npartitions):
    df['hash_column'] = df[join_key] % npartitions
    return df


@delayed
def perform_join(block1, block2, join_key):
    # Perform the join operation
    join_result = dd.merge(block1, block2, on=join_key, how='inner')
    return join_result


class CustomJoinPipelines:

    def __init__(self):
        pass

    def pipelined_hash_join(self, df1, df2, join_key, npartitions):
        print(f"The number of partitions calculated {npartitions}")

        start = time.time()

        df1 = dd.from_pandas(df1, npartitions=npartitions)
        df2 = dd.from_pandas(df2, npartitions=npartitions)

        # Define the metadata for the DataFrame
        meta_sql = pd.DataFrame(columns=df1.columns.tolist() + ['hash_column'])
        meta_redis = pd.DataFrame(columns=df2.columns.tolist() + ['hash_column'])

        df1 = df1.map_partitions(assign_partitions, join_key, npartitions, meta=meta_sql)
        df2 = df2.map_partitions(assign_partitions, join_key, npartitions, meta=meta_redis)

        df1 = df1.drop(columns=['hash_column'])
        df2 = df2.drop(columns=['hash_column'])

        # Split df1 and df2 into blocks
        blocks_df1 = df1.to_delayed()
        blocks_df2 = df2.to_delayed()

        timestamp_constraint = datetime.now() - relativedelta(years=2)

        # Concatenate the join results
        final_result = [
            perform_join(
                block1[block1['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']],
                block2[block2['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']],
                join_key
            )
            for block1, block2 in zip(blocks_df1, blocks_df2)
        ]
        # Compute and display the final result
        final_result = dd.compute(*final_result, num_workers=4)

        finish = time.time() - start

        print(f"Execution time {finish:.2f} seconds")
        return (final_result)


if __name__ == '__main__':
    conn = sqlite3.connect('data1/mydatabase.db')
    cursor = conn.cursor()

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    start = time.time()

    # Execute a SELECT query to fetch all rows from a table
    # redis_data = r.hgetall('dataset100k')
    redis_data = r.hgetall('dataset1kk')

    redis_df = redis_to_pandas(redis_data)

    # Execute a SELECT query to fetch all rows from a table
    # cursor.execute("SELECT * FROM dataset100k")
    cursor.execute("SELECT * FROM dataset1kk")

    sqlite_data = cursor.fetchall()
    sql_df = sql_to_pandas(sqlite_data)

    finish = time.time() - start
    print(f"Reading and Foramming the data in {finish:.2f} seconds")

    solution = CustomJoinPipelines.pipelined_hash_join(df1=redis_df, df2=sql_df, join_key='user_id', npartitions=100)

    # # Create a profile object
    # profile = cProfile.Profile()
    #
    # # Start profiling
    # profile.enable()
    #
    # # Call the function or code you want to profile
    # myfunc()
    #
    # # Stop profiling
    # profile.disable()
    #
    # # Print the profiling results
    # profile.print_stats()
