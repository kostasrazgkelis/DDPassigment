import random
import sqlite3
import time
from functools import wraps

from faker import Faker


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'SQLite3: Function {func.__name__} Took {total_time:.4f} seconds')

        return result

    return timeit_wrapper


@timeit
def insert_data(name, num_rows):
    batch_size = 10000

    cursor.execute(f"DROP TABLE IF EXISTS {name}")

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {name} (counter INTEGER PRIMARY KEY, user_id INT, timestamp TEXT)")

    # Prepare the INSERT statement
    insert_query = f"INSERT INTO {name} (user_id, timestamp) VALUES (?, ?)"

    # Generate and insert the data in batches
    for batch_number, _ in enumerate(range(0, num_rows, batch_size)):
        data_batch = []
        for _ in range(batch_size):
            user_id = random.randint(10000, 10010)
            timestamp = fake.date_time_between(start_date='-10y', end_date='now')
            data_batch.append((user_id, timestamp))

        cursor.executemany(insert_query, data_batch)
        conn.commit()


if __name__ == "__main__":
    fake = Faker()

    # Connect to the first SQLite database
    conn = sqlite3.connect('data1/mydatabase.db')
    cursor = conn.cursor()

    insert_data(name='dataset1k', num_rows=1000)
    insert_data(name='dataset100K', num_rows=100_000)
    insert_data(name='dataset250K', num_rows=250_000)

    # Close the database connection
    conn.close()
