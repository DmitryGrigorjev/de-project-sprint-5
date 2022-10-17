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
    dag_id='dds_to_fct_dag.py',
    schedule_interval="0 0 * * *",
    default_args=args
)

def dds_to_fct_dag() -> None:
    connection_fct = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de") 
    with connection_fct.cursor() as cur_fct:
        cur_fct.execute('select coalesce(max(fct_ts),\'2022-09-01 00:00:00\') from dds.fct_courier_deliveries')
        inc_ts=cur_fct.fetchone()        
        cur_fct.execute(""" insert into dds.fct_courier_deliveries (id_courier,id_delivery,delivery_sum,delivery_tip_sum,delivery_rate, fct_ts)
                            select 
                            (select id  from dds.dds_couriers dc where dc.courier_id=dd.courier_id) id_courier,
                            -- (select id  from dds.dds_restaurants dr where dr.restaurant_id =dd.restaurant_id) id_restaurant,
                            id id_delivery,
                            sum,
                            tip_sum, 
                            rate,
                            order_ts
                            from dds.dds_deliveries dd
                            where order_ts > %(ts)s
                            and (select id  from dds.dds_couriers dc where dc.courier_id=dd.courier_id) is not null
                    """,
                    {
                        "ts": inc_ts
                    }
                    )
    connection_fct.commit()
    connection_fct.close()
 
dds_to_fct_dag = PythonOperator(task_id='dds_to_fct_dag',
                            python_callable=dds_to_fct_dag,
                            dag=dag)

dds_to_fct_dag