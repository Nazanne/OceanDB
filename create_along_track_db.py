import psycopg2
from psycopg2 import sql

# Connection details
host = "localhost"
user = "postgres"
port = "5432"  # typically 5432 for PostgreSQL

# Connect to the default database
conn = psycopg2.connect(dbname="postgres", user=user, host=host, port=port)
conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

# Create a cursor object
cur = conn.cursor()

# Define the new database name
new_database_name = "along-track"

# Create the new database
cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_database_name)))
cur.execute(sql.SQL("CREATE EXTENSION postgis;"))
            
# Close the cursor and connection
cur.close()
conn.close()

print(f"Database '{new_database_name}' created successfully.")
