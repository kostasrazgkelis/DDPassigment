import redis
from faker import Faker


fake = Faker()

# Connect to Redis
r = redis.Redis(host='localhost', port=6379)

# Delete the "users" key if it exists
r.delete('dataset1')
r.delete('dataset2')

for _ in range(0, 100):
    timestamp = fake.date_time_between(start_date='-1y', end_date='now')
    user_id = str(_)
    user_data = {
        'timestamp': timestamp,
        'name': fake.name(),
        'email': fake.email()
    }
    r.hset('dataset1', user_id, str(user_data))

for _ in range(0, 100):
    timestamp = fake.date_time_between(start_date='-1y', end_date='now')
    user_id = str(_)
    user_data = {
        'timestamp': timestamp,
        'name': fake.name(),
        'email': fake.email()
    }
    r.hset('dataset2', user_id, str(user_data))

# Close the Redis connection
r.close()
