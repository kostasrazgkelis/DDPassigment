import random

import redis
from faker import Faker

fake = Faker()

# Connect to Redis
r = redis.Redis(host='localhost', port=6379)

# Delete the "users" key if it exists
r.delete('test_dataset')

# Create a pipeline
pipe = r.pipeline()

num_entries = 100
batch_size = 100

# Insert data into Redis using pipeline
for i in range(num_entries):
    timestamp = fake.date_time_between(start_date='-10y', end_date='now')
    user_data = {
        'user_id': random.randint(10000, 10010),
        'timestamp': timestamp,
    }
    pipe.hset('test_dataset', i, str(user_data))

    # Execute the pipeline in batches
    if (i + 1) % batch_size == 0:
        pipe.execute()
        pipe = r.pipeline()
        print(f"The batch number {i // batch_size} has finished")

# Execute the pipeline
pipe.execute()
r.close()
