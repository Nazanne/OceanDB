import netCDF4 as nc
import psycopg2 as ps
from psycopg2 import sql

class AlongTrackDatabase:
    dbName = 'along-track'
    
    def __init__(self,host,username,port = "5432"):
        self.host = host
        self.username = username
        self.port = port

    def connection(self):
        conn = ps.connect(host=self.host,
            dbname=self.dbName,
            user=self.username
        )
        return conn

    def createDatabase(self):
        conn = ps.connect(dbname="postgres", user=self.username, host=self.host, port=self.port)
        conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

        # Create a cursor object
        cur = conn.cursor()

        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = '{self.dbName}'"))
        exists = cur.fetchone()
        if exists is not None:
            print(f"Database '{self.dbName}' already exists.")  
        else:
            # Create the new database
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbName)))
            cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
            print(f"Database '{self.dbName}' created successfully.")

        # Close the cursor and connection
        cur.close()
        conn.close()
 

    def dropDatabase(self):
        conn = ps.connect(dbname="postgres", user=self.username, host=self.host, port=self.port)
        conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

        # Create a cursor object
        cur = conn.cursor()

        # Create the new database
        cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.dbName)))
                
        # Close the cursor and connection
        cur.close()
        conn.close()

        print(f"Database '{self.dbName}' dropped.") 

