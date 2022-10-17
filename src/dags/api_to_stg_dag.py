from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from dateutil.parser import parse
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt
import requests
import sqlalchemy as sa
import os
import psycopg2, psycopg2.extras
import json

args = {
   'owner': 'airflow',
   'start_date': dt.datetime(2022, 10, 10),
   'retries': 0,
   'retry_delay': dt.timedelta(minutes=1),
    'catchup': False,
}

dag = DAG (
    dag_id='api_to_stg_dag.py',
    schedule_interval="0 0 * * *",
    default_args=args
)

def api_to_stg(conn: str, method: str, table: str) -> None:

    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    c = BaseHook.get_connection(conn)
    headers = {
                "X-API-KEY": '25c27781-8fde-4b30-a22e-524044a7580f',
                "X-Nickname": 'grigorjev.d.e',
                "X-Cohort": '5'
                }
    
    if table == 'restaurants':
        r = requests.get('https://'+c.host+method+'?sort_field=id&sort_direction=asc&limit=50&offset=0', headers=headers)
        data = r.text.replace('_','')
        rest_json = json.loads(data)
        with pg.cursor() as cur:
            for r in rest_json:
                cur.execute("""
                            insert into stg.restaurants (restaurant_info) 
                            values (%(value)s) 
                            """,
                            {
                                "value": '['+str(r).replace('\'','\"')+']',
                            }
                )
        pg.commit()
    elif table=='couriers':
        r = requests.get('https://'+c.host+method+'?sort_field=id&sort_direction=asc&limit=50&offset=0', headers=headers)
        data = r.text.replace('_','')
        rest_json = json.loads(data)
        with pg.cursor() as cur:
            for r in rest_json:
                cur.execute("""
                            insert into stg.couriers (courier_info) 
                            values (%(value)s) 
                            """,
                            {
                                "value": '['+str(r).replace('\'','\"')+']',
                            }
                )
        pg.commit()
    elif table=='deliveries':
        r = requests.get('https://'+c.host+method+'?sort_field=id&sort_direction=asc&limit=50&offset=0', headers=headers)
        data = r.text.replace('_','')
        rest_json = json.loads(data)
        with pg.cursor() as cur:
            for r in rest_json:
                # я сначала сделал свой тестовый набор данных (см. test_data.sql), сделал хранилище, а в самом конце уже стал получать файлы по апи
                # формат оказался немного разные, но я подогнал replace-ом названия полей в json, работает и с моим набором и с апишным
                # просто так мне было удобнее
                # и кстати restaurants_id никак не привязаны в апишном наборе, а в моем я сделал с этим полем
                cur.execute("""
                            insert into stg.deliveries (delivery_info) 
                            values (%(value)s) 
                            """,
                            {
                                "value": '['+str(r).replace('\'','\"').replace('orderid','order_id').replace('deliveryid','delivery_id').replace('courierid','courier_id').replace('deliveryts','delivery_ts').replace('tipsum','tip_sum')+']',
                            }
                )
        pg.commit()

def api_to_stg_dag():
    api_to_stg('create_files_api','/restaurants','restaurants')
    api_to_stg('create_files_api','/couriers','couriers')
    api_to_stg('create_files_api','/deliveries','deliveries')

api_to_stg_dag = PythonOperator(task_id='api_to_stg_dag',
                            python_callable=api_to_stg_dag,
                            dag=dag)

api_to_stg_dag