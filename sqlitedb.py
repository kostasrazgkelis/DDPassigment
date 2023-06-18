import random
import sqlite3
from faker import Faker


fake = Faker()

# Connect to the first SQLite database
conn = sqlite3.connect('data1/mydatabase.db')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS test_dataset")


cursor.execute("CREATE TABLE IF NOT EXISTS test_dataset (counter INTEGER PRIMARY KEY, user_id INT, timestamp TEXT)")

# Prepare the INSERT statement
insert_query = "INSERT INTO test_dataset (user_id, timestamp) VALUES (?, ?)"

num_rows = 100
batch_size = 100

# Generate and insert the data in batches
for batch_number, _ in enumerate(range(0, num_rows, batch_size)):
    data_batch = []
    for _ in range(batch_size):
        user_id = random.randint(10000, 10010)
        timestamp = fake.date_time_between(start_date='-10y', end_date='now')
        data_batch.append((user_id, timestamp))

    cursor.executemany(insert_query, data_batch)
    conn.commit()
    print(f"The batch number {batch_number} has finished")

# Close the database connection
conn.close()