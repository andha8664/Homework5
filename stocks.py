# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(url):
    f = requests.get(url)
    return (f.json())


@task
def transform(data):
    results = [] # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
    for d in data["Time Series (Daily)"]: # here d is a date: "YYYY-MM-DD"
        stock_info = data["Time Series (Daily)"][d]
        stock_info['6. date'] = d
        results.append(stock_info)
        if len(results)>89:
          break
    return results

@task
def load(con, records, target_table):
    try:
        con.execute("BEGIN;")
        con.execute(f"DROP TABLE IF EXISTS {target_table};")
        con.execute(f"CREATE OR REPLACE TABLE {target_table} (open float, high float, low float, close float, volume int, date DATE primary key);")
        for r in records:
            open = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]
            date = r["6. date"]
            sql = f"INSERT INTO {target_table} (open, high, low, close, volume, date) VALUES ({open}, {high}, {low}, {close}, {volume}, '{date}')"
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'stocks',
    #start_date = datetime(2024,9,21),
    start_date = datetime.now(),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "stock_db.raw_data.stock_price"
    url = Variable.get("stock_url")
    cur = return_snowflake_conn()

    data = extract(url)
    lines = transform(data)
    load(cur, lines, target_table)
