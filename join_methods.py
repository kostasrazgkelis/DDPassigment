import sqlite3
import sys
import time
from datetime import datetime
from functools import wraps

from bitarray import bitarray
from pybloom_live import BloomFilter
import warnings

import redis
import pandas as pd
import ast
import dask.dataframe as dd
from dask import delayed
import cProfile

from dask.tests.test_system import psutil
from dateutil.relativedelta import relativedelta
from dask.diagnostics import visualize

warnings.filterwarnings("ignore", category=Warning)

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


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')

        return result

    return timeit_wrapper


class CustomJoinPipelines:

    def __init__(self):
        self.capacity = 0

    @delayed
    def perform_join(self, block1, block2, join_key):
        # Perform the join operation
        join_result = dd.merge(block1, block2, on=join_key, how='inner')
        return join_result

    @timeit
    def normal_join(self, df1, df2, join_key):
        # Assuming df1 and df2 are Pandas DataFrames
        timestamp_constraint = datetime.now() - relativedelta(years=2)

        # Apply the timestamp constraint and select columns
        filtered_df1 = df1[df1['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']]
        filtered_df2 = df2[df2['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']]

        # Perform the join operation
        final_result = filtered_df1.merge(filtered_df2, on='user_id', how='inner')

        return final_result

    @timeit
    def pipelined_hash_join(self, df1, df2, join_key, npartitions):

        df1 = dd.from_pandas(df1, npartitions=npartitions)
        df2 = dd.from_pandas(df2, npartitions=npartitions)

        df1['hash_value'] = df1['user_id'].apply(lambda x: x % npartitions)
        df2['hash_value'] = df2['user_id'].apply(lambda x: x % npartitions)

        # Set "hash_value" column as the index
        df1 = df1.set_index('hash_value')
        df2 = df2.set_index('hash_value')

        timestamp_constraint = datetime.now() - relativedelta(years=2)

        # Iterate over the blocks of data
        for block1, block2 in zip(df1.partitions, df2.partitions):
            # Perform the join operation
            merged_data = self.perform_join(
                block1[block1['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']],
                block2[block2['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']],
                join_key
            )
            # Print the merged data
            merged_data.compute()


    @timeit
    def semi_join(self, df1, df2, join_key, npartitions):

        df1 = dd.from_pandas(df1, npartitions=npartitions)
        df2 = dd.from_pandas(df2, npartitions=npartitions)

        timestamp_constraint = datetime.now() - relativedelta(years=2)

        df1 = df1.reset_index(drop=True)
        df1 = df1.drop(columns='counter')

        df2 = df2.reset_index(drop=True)
        df2 = df2.drop(columns='counter')

        # Apply the timestamp constraint and select columns
        df1 = df1[df1['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']]
        df2 = df2[df2['timestamp'] >= timestamp_constraint][['user_id', 'timestamp']]

        df1 = df1.set_index(join_key).repartition(npartitions=npartitions)
        df2 = df2.set_index(join_key).repartition(npartitions=npartitions)

        # Iterate over the blocks of data
        for block1, block2 in zip(df1.partitions, df2.partitions):
            # Perform the join operation
            merged_data = dd.merge(
                block1[block1['timestamp'] >= timestamp_constraint],
                block2[block2['timestamp'] >= timestamp_constraint],
                left_index=True, right_index=True, how='inner'
            )
            # Print the merged data
            merged_data.compute()

    def create_bloom_filter(self, partition):
        bloom_filter = BloomFilter(capacity=self.capacity, error_rate=0.1)
        partition.loc[:, 'user_id'] = partition['user_id'].astype("string")
        partition['user_id'].apply(bloom_filter.add)
        return bloom_filter

    def merge_bloom_filters(self, bloom_filters):
        bit_arrays = [pd.Series(bloomf.bitarray) for bloomf in bloom_filters.compute()]

        # Perform union using a loop
        union_bit_array = bit_arrays[0]
        for bit_array in bit_arrays[1:]:
            union_bit_array |= bit_array

        final_bloom_filter = BloomFilter(capacity=self.capacity, error_rate=0.1)
        final_bloom_filter.bitarray = bitarray(union_bit_array.astype(bool).tolist())

        return final_bloom_filter

    @timeit
    def intersection_bloom_filter_join(self, df1, df2, join_key, npartitions):
        start = time.time()

        df1[join_key] = df1[join_key].astype('string')
        df2[join_key] = df1[join_key].astype('string')

        df1 = dd.from_pandas(df1, npartitions=npartitions)
        df2 = dd.from_pandas(df2, npartitions=npartitions)

        self.capacity = max([df1['user_id'].compute().unique().shape[0], df2['user_id'].compute().unique().shape[0]])

        bloom_filter1 = df1.map_partitions(self.create_bloom_filter, meta=pd.DataFrame(columns=df1.columns))
        bloom_filter2 = df2.map_partitions(self.create_bloom_filter, meta=pd.DataFrame(columns=df2.columns))

        merged_bloom_fitlers = self.merge_bloom_filters(bloom_filter1).intersection(
            self.merge_bloom_filters(bloom_filter2))

        print(f"total time to build the filter {time.time() - start}")

        timestamp_constraint = datetime.now() - relativedelta(years=2)

        df1 = \
        df1[(df1[join_key].apply(lambda x: x in merged_bloom_fitlers)) & (df1['timestamp'] >= timestamp_constraint)][
            [join_key, 'timestamp']]
        df2 = \
        df2[(df2[join_key].apply(lambda x: x in merged_bloom_fitlers)) & (df2['timestamp'] >= timestamp_constraint)][
            [join_key, 'timestamp']]

        # Iterate over the blocks of data
        for block1, block2 in zip(df1.partitions, df2.partitions):
            # Perform the join operation
            merged_data = dd.merge(
                block1[block1['timestamp'] >= timestamp_constraint],
                block2[block2['timestamp'] >= timestamp_constraint],
                left_index=True, right_index=True, how='inner'
            )
            # Print the merged data
            merged_data.compute()



if __name__ == '__main__':

    # Check if command-line arguments are provided

    if not len(sys.argv) > 0:
        print("No arguments provided.")
        sys.exit(1)

    dataset_name, join_method = sys.argv[1:]



    conn = sqlite3.connect('data1/mydatabase.db')
    cursor = conn.cursor()

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_data = r.hgetall(dataset_name)

    cursor.execute(f"SELECT * FROM {dataset_name}")
    sqlite_data = cursor.fetchall()

    redis_df = redis_to_pandas(redis_data)

    sql_df = sql_to_pandas(sqlite_data)

    pipeLineObj = CustomJoinPipelines()

    if join_method == "hash_join":
        pipeLineObj.pipelined_hash_join(df1=redis_df, df2=sql_df, join_key='user_id', npartitions=100)

    elif join_method == "semi_join":
        pipeLineObj.semi_join(df1=redis_df, df2=sql_df, join_key='user_id', npartitions=100)

    elif join_method == "bloom_join":
        pipeLineObj.intersection_bloom_filter_join(df1=redis_df, df2=sql_df, join_key='user_id', npartitions=100)

    else:
        print("Invalid join method specified.")
        sys.exit(1)


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
