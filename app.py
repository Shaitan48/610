import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def process_data(engine):
    conn = engine.connect()

    data = pd.read_sql('select * from test_table', conn)

    res = data.where(data['name'].apply(len) < 6).dropna(how='any').groupby('name').agg({'age': ['min', 'max']})

    return res

if __name__ == '__main__':
    db_user = 'postgres'
    db_pass = 'password'
    db_host = 'db'
    db_port = '5432'
    db_name = 'test_db'

    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    result = process_data(engine)

    print(result)