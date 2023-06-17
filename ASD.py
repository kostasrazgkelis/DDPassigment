import sqlite3
import redis
import pandas as pd
from datetime import *
import ast
import dask.dataframe as dd
from dask import delayed
import hashlib

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

    result = []
    for _, row in df.iterrows():
        result.append(
            ast.literal_eval(row["raw_data"].replace('datetime.datetime', '').replace("(", '"').replace(")", '"')))
    df.drop(columns=['raw_data'], inplace=True)

    # Convert the string to a dictionary while preserving the datetime object
    df = pd.DataFrame(result)
    df.index = index_df

    df["timestamp"] = df["timestamp"].apply(lambda x: udf_reformat_to_iso(x))
    df.reset_index(drop=False, inplace=True, names='key')
    return df

def  sql_to_pandas(data) -> pd.DataFrame:
    df = pd.DataFrame(data, columns=['key', 'timestamp', 'name', 'email'])
    return df

@delayed
def perform_join(block1, block2):
    # Perform the join operation
    join_result = dd.merge(block1, block2, on='hash_column', how='inner')
    return join_result

conn = sqlite3.connect('data1/mydatabase.db')
cursor = conn.cursor()

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


# Execute a SELECT query to fetch all rows from a table
redis_data = r.hgetall('dataset1')
redis_df = redis_to_pandas(redis_data)


# Execute a SELECT query to fetch all rows from a table
cursor.execute("SELECT * FROM dataset1")
sqlite_data = cursor.fetchall()
sql_df = sql_to_pandas(sqlite_data)

join_key = 'name'

md5_hash = hashlib.md5()
# hash_object.update(string_to_hash.encode('utf-8'))
# hashed_string = hash_object.hexdigest()


npartitions = 100

sql_df['hash_column'] = sql_df[join_key].apply(lambda x: md5_hash.update(x.encode('utf-8')) or md5_hash.hexdigest())
redis_df['hash_column'] = sql_df[join_key].apply(lambda x: md5_hash.update(x.encode('utf-8')) or md5_hash.hexdigest())

df1 = dd.from_pandas(sql_df, npartitions=10)
df2 = dd.from_pandas(redis_df, npartitions=10)

# Split df1 into blocks
blocks_df1 = df1.to_delayed()

# Split df2 into blocks
blocks_df2 = df2.to_delayed()

# Concatenate the join results
final_result = [perform_join(block1, block2) for block1, block2 in zip(blocks_df1, blocks_df2)]

# Compute and display the final result
final_result = dd.compute(*final_result)
print(final_result)

# joined_results = dd.concat([dd.merge(block1, block2, on='hash_column') for block1, block2 in zip(blocks_df1, blocks_df2)]).compute()
# print(joined_results)