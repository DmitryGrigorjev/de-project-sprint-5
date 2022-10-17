from email.quoprimime import quote
from sqlite3 import Timestamp
from unicodedata import numeric
from xmlrpc.client import DateTime
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from dateutil.parser import parse
from psycopg.rows import class_row
from pydantic import BaseModel
from typing import List, Optional

import datetime as dt
import requests
import sqlalchemy as sa
import os
import psycopg2, psycopg2.extras
import json

class FirstModel(BaseModel):
    object_id: str
    object_value: str

class SecondModel(BaseModel):
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    restaurant_id: str
    address: str
    rate: float
    sum: float
    tip_sum: float

args = {
   'owner': 'airflow',
   'start_date': dt.datetime(2022, 10, 10),
   'retries': 0,
   'retry_delay': dt.timedelta(minutes=1),
    'catchup': False,
}

dag = DAG (
    dag_id='stg_to_dds_dag.py',
    schedule_interval="0 0 * * *",
    default_args=args
)

def stg_to_dds_couriers_get() -> List[FirstModel]:
  
    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with pg.cursor() as cur_couriers:
        cur_couriers.execute("""
                    select courier_info from stg.couriers
                    """
                    )
        objs=cur_couriers.fetchall()
        return objs
    
def stg_to_dds_couriers_push(objs: FirstModel) -> None:

    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with pg.cursor() as cur_couriers:
        for r in objs:
            couriers_json=json.loads(str(r[0]).replace('[','').replace(']',''))
            cur_couriers.execute("""
                                insert into dds.dds_couriers (courier_id, courier_name) 
                                values (%(id)s, %(value)s) 
                                on conflict (courier_id) do update set courier_name = EXCLUDED.courier_name
                                """,
                                {
                                    "id": str(couriers_json['id']),
                                    "value": str(couriers_json['name']),
                                }
            )
            pg.commit()
            
def stg_to_dds_couriers() -> None:
    objs=stg_to_dds_couriers_get()
    stg_to_dds_couriers_push(objs)

def stg_to_dds_restaurants_get() -> List[FirstModel]:
  
    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with pg.cursor() as cur_restaurants:
        cur_restaurants.execute("""
                    select restaurant_info from stg.restaurants
                    """
                    )
        objs=cur_restaurants.fetchall()
        return objs
    
def stg_to_dds_restaurants_push(objs: FirstModel) -> None:

    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with pg.cursor() as cur_restaurants:
        for r in objs:
            restaurants_json=json.loads(str(r[0]).replace('[','').replace(']',''))
            cur_restaurants.execute("""
                                insert into dds.dds_restaurants (restaurant_id, restaurant_name) 
                                values (%(id)s, %(value)s) 
                                on conflict (restaurant_id) do update set restaurant_name = EXCLUDED.restaurant_name
                                """,
                                {
                                    "id": str(restaurants_json['id']),
                                    "value": str(restaurants_json['name']),
                                }
            )
            pg.commit()
            
def stg_to_dds_restaurants() -> None:
    objs=stg_to_dds_restaurants_get()
    stg_to_dds_restaurants_push(objs)


def stg_to_dds_deliveries_get() -> List[SecondModel]:
  
    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with pg.cursor() as cur_deliveries:
        cur_deliveries.execute("""
                    select delivery_info from stg.deliveries
                    where update_ts > (select coalesce(max(update_ts),\'2022-09-01 00:00:00\') from dds.dds_deliveries)
                    """
                    )
        objs=cur_deliveries.fetchall()
        return objs

def stg_to_dds_deliveries_push(objs: SecondModel) -> None:

    pg = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with pg.cursor() as cur_deliveries:
        for r in objs:
            deliveries_json=json.loads(str(r[0]).replace('[','').replace(']',''))
            cur_deliveries.execute("""
                                insert into dds.dds_deliveries (order_id, 
                                                                order_ts, 
                                                                delivery_id, 
                                                                courier_id, 
                                                                address, 
                                                                rate, 
                                                                sum, 
                                                                tip_sum) 
                                values (%(order_id)s, 
                                        %(order_ts)s, 
                                        %(delivery_id)s, 
                                        %(courier_id)s, 
                                        %(address)s, 
                                        %(rate)s, 
                                        %(sum)s, 
                                        %(tip_sum)s) 
                                """,
                                {
                                    "order_id": str(deliveries_json['order_id']), 
                                    "order_ts": deliveries_json['delivery_ts'], 
                                    "delivery_id": str(deliveries_json['delivery_id']), 
                                    "courier_id": str(deliveries_json['courier_id']), 
                                    "address": str(deliveries_json['address']), 
                                    "rate": deliveries_json['rate'], 
                                    "sum": deliveries_json['sum'], 
                                    "tip_sum": deliveries_json['tip_sum']
                                }
                        )
            pg.commit()

def stg_to_dds_deliveries() -> None: 
    objs=stg_to_dds_deliveries_get()
    stg_to_dds_deliveries_push(objs)

def stg_to_dds_dag() -> None:
    stg_to_dds_couriers()
    stg_to_dds_restaurants()
    stg_to_dds_deliveries()

stg_to_dds_dag = PythonOperator(task_id='stg_to_dds_dag',
                            python_callable=stg_to_dds_dag,
                            dag=dag)

stg_to_dds_dag