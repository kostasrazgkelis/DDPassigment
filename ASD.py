import sqlite3
import redis
import pandas as pd
from datetime import *
import ast

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

join_key = 'key'

# Step 2: Partition the DataFrames
df1_partitions = sql_df.groupby(join_key)
df2_partitions = redis_df.groupby(join_key)

print('end')