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
    dag_id='fct_to_dm_dag.py',
    schedule_interval="0 0 * * *",
    default_args=args
)

# признаюсь честно в пайтоне я не силен. поэтому сделано простенько, без классов и именованных параметров в запросах как у создателей курса

def fct_to_dm_dag() -> None:
#курьеры    
    connection = psycopg2.connect(user="jovyan",password="jovyan",host="localhost",port="5432",database="de")
 
    with connection.cursor() as cur:
        cur.execute("""
                                truncate table cdm.dm_courier_ledger restart identity cascade;
                                insert into cdm.dm_courier_ledger (courier_id, 
                                                                    courier_name, 
                                                                    settlement_year,
                                                                    settlement_month,
                                                                    orders_count,
                                                                    orders_total_sum,
                                                                    rate_avg,
                                                                    orders_processing_fee,
                                                                    courier_orders_sum,
                                                                    courier_tips_sum,
                                                                    courier_reward_sum) 
                                select  
                                    a.courier_id,
                                    a.courier_name,
                                    a.settlement_year,
                                    a.settlement_month,
                                    a.orders_count,
                                    a.orders_total_sum,
                                    a.rate_avg,
                                    a.order_processing_fee,
                                    round(a.orders_total_sum*a.courier_processing_fee,2) courier_order_sum,
                                    a.courier_tips_sum,
                                    round((a.courier_processing_fee + a.courier_tips_sum*.95),2) courier_reward_sum 
                                from (select 
                                        fcd.id_courier courier_id,
                                        dc.courier_name,
                                        date_part('year',dd.order_ts) settlement_year,
                                        date_part('month',dd.order_ts) settlement_month,
                                        count(*) orders_count,
                                        sum(fcd.delivery_sum) orders_total_sum,
                                        round(avg(fcd.delivery_rate*1.00),2) rate_avg,
                                        sum(fcd.delivery_sum*0.25) order_processing_fee,
                                        case  
                                            when avg(fcd.delivery_rate) between 4.9 and 5 then greatest(sum(fcd.delivery_sum)*.1,200)
                                            when avg(fcd.delivery_rate) between 4.5 and 4.9 then greatest(sum(fcd.delivery_sum)*.08,175)
                                            when avg(fcd.delivery_rate) between 4 and 4.5 then greatest(sum(fcd.delivery_sum)*.07,150)
                                            else greatest(sum(fcd.delivery_sum)*.05,100)
                                        end as courier_processing_fee,
                                        SUM(fcd.delivery_tip_sum) courier_tips_sum 
                                        from dds.fct_courier_deliveries fcd 
                                        join dds.dds_couriers dc on dc.id =fcd.id_courier 
                                        join dds.dds_deliveries dd on dd.id =fcd.id_delivery 
                                        group by 
                                        fcd.id_courier,
                                        dc.courier_name,
                                        date_part('year',dd.order_ts),
                                        date_part('month',dd.order_ts)
                            ) a
                    """
                    )
    connection.commit()
    connection.close()
    
fct_to_dm_dag = PythonOperator(task_id='fct_to_dm_dag',
                            python_callable=fct_to_dm_dag,
                            dag=dag)

fct_to_dm_dag