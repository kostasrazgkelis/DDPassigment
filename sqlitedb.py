import sqlite3
from faker import Faker


fake = Faker()

# Connect to the first SQLite database
conn = sqlite3.connect('data1/mydatabase.db')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS dataset1")
cursor.execute("DROP TABLE IF EXISTS dataset2")

cursor.execute("CREATE TABLE IF NOT EXISTS dataset1 (id INTEGER PRIMARY KEY, timestamp TEXT, name TEXT, email TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS dataset2 (id INTEGER PRIMARY KEY, timestamp TEXT, name TEXT, email TEXT)")

for _ in range(0, 100):
    timestamp = fake.date_time_between(start_date='-1y', end_date='now')
    cursor.execute("INSERT INTO dataset1 (timestamp, name, email) VALUES (? , ?, ?)", (timestamp, fake.name(), fake.email()))
    conn.commit()

for _ in range(0, 100):
    timestamp = fake.date_time_between(start_date='-1y', end_date='now')
    cursor.execute("INSERT INTO dataset2 (timestamp, name, email) VALUES (? , ?, ?)", (timestamp, fake.name(), fake.email()))
    conn.commit()

# Close the database connections
conn.close()
