import random
import time
from functools import wraps

import redis
from faker import Faker


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Redis: Function {func.__name__}  Took {total_time:.4f} seconds')

        return result

    return timeit_wrapper


@timeit
def insert_data(name, num_rows):
    # Delete the "users" key if it exists
    r.delete(name)

    # Create a pipeline
    pipe = r.pipeline()
    batch_size = 10000

    # Insert data into Redis using pipeline
    for i in range(num_rows):
        timestamp = fake.date_time_between(start_date='-10y', end_date='now')
        user_data = {
            'user_id': random.randint(10000, 10010),
            'timestamp': timestamp,
        }
        pipe.hset(name, i, str(user_data))

        # Execute the pipeline in batches
        if (i + 1) % batch_size == 0:
            pipe.execute()
            pipe = r.pipeline()

    # Execute the pipeline
    pipe.execute()


if __name__ == "__main__":
    fake = Faker()
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379)

    insert_data(name='dataset1K', num_rows=1000)
    insert_data(name='dataset100K', num_rows=100_000)
    insert_data(name='dataset250K', num_rows=250_000)
    insert_data(name='dataset500K', num_rows=500_000)
    insert_data(name='dataset1000K', num_rows=1_000_000)

    r.close()
