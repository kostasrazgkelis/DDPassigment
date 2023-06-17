import sqlite3
from faker import Faker


fake = Faker()

# Connect to the first SQLite database
conn = sqlite3.connect('data1/mydatabase.db')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS dataset1")
#cursor.execute("DROP TABLE IF EXISTS dataset2")

cursor.execute("CREATE TABLE IF NOT EXISTS dataset1 (id INTEGER PRIMARY KEY, timestamp TEXT, name TEXT, email TEXT)")
#cursor.execute("CREATE TABLE IF NOT EXISTS dataset2 (id INTEGER PRIMARY KEY, timestamp TEXT, name TEXT, email TEXT)")


# Set the number of rows to insert
num_rows = 1000000
batch_size = 1000

# Prepare the INSERT statement
insert_query = "INSERT INTO dataset1 (timestamp, name, email) VALUES (?, ?, ?)"


# Generate and insert the data in batches
for _ in range(0, num_rows, batch_size):
    data_batch = []
    for _ in range(batch_size):
        timestamp = fake.date_time_between(start_date='-1y', end_date='now')
        name = fake.name()
        email = fake.email()
        data_batch.append((timestamp, name, email))

    cursor.executemany(insert_query, data_batch)
    conn.commit()

# for _ in range(0, 1000):
#     timestamp = fake.date_time_between(start_date='-1y', end_date='now')
#     cursor.execute("INSERT INTO dataset2 (timestamp, name, email) VALUES (? , ?, ?)", (timestamp, fake.name(), fake.email()))
#     conn.commit()

# Close the database connections
conn.close()
