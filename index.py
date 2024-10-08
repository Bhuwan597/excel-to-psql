import psycopg2
import pandas as pd
from psycopg2 import sql, extensions
from sqlalchemy import create_engine

df = pd.read_excel("files/example.xlsx")

def main():
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="root"
    )
    # Creating the test database if not exists
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", ("test",))
    exists = cur.fetchone()
     
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("test")))
        print(f"Database 'test' created!")
    else:
        print(f"Database 'test' already exists.")


    # Creating the data table with available columns if not exists
    columns = df.columns
    create_table_query = f"CREATE TABLE IF NOT EXISTS data ({', '.join([f'{col} TEXT' for col in columns])});"
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully!")
    print("Connected to database")

    for index,row in df.iterrows():
        insert_query = f"""INSERT INTO data ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))}); """
        cur.execute(insert_query, tuple(row[col] for col in df.columns))
        print(f"{index + 1} data inserted")

    conn.commit()
    cur.close()
    conn.close()

    print("All data inserted successfully!")


def another_method():
    df = pd.read_excel("files/example.xlsx")

    DATABASE_URL = "postgresql://postgres:root@localhost:5432"

    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine(DATABASE_URL)

    df.to_sql('data', engine, if_exists='replace', index=False)

    print("Data inserted successfully!")


# main()
# another_method()