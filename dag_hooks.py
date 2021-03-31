import logging
import pandas as pd
import ast

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator



default_args = {
        "owner": "gizelly",
        'start_date':datetime(2020,11,10)
        }



def pull_and_load(**context):

        ti = context['task_instance']

        all_data = []

        info_clima = ti.xcom_pull(task_ids='clima_api_call')
        clima_literal_eval = ast.literal_eval(info_clima)

        for chave, valor in clima_literal_eval[0].items():
            all_data.append(valor)


        info_dollar = ti.xcom_pull(task_ids='dollar_api_call')
        # ast.literal_eval é mágico!!!
        dollar_literal_eval = ast.literal_eval(info_dollar)

        all_data.append(dollar_literal_eval['USD']['code'])
        all_data.append(dollar_literal_eval['USD']['codein'])
        all_data.append(dollar_literal_eval['USD']['name'])
        all_data.append(dollar_literal_eval['USD']['high'])


        conn = PostgresHook('postgres_weatherdb')

        insert_in_db = """INSERT INTO airlflow_dollar_clima
                        (code,codein,name,high,country,date,text)
                        VALUES
                        (%s,%s,%s,%s,%s,%s,%s);"""

        conn.run(insert_in_db,parameters=all_data)


#def pull_in_airflow(**context):
    #ti = context['task_instance']

    #select_limit_10 = ti.xcom_pull(task_ids='select_10_from_table')

    #print(select_limit_10)




###### DAG #########

with DAG(dag_id='dag_hooks',
        default_args=default_args,
        schedule_interval = timedelta(minutes=10)
        ) as dag:

        start = DummyOperator(task_id='start')

        dollar_api_call = SimpleHttpOperator(task_id='dollar_api_call',
                                             http_conn_id='economia_awesomeapi',
                                             endpoint='all/USD-BRL',
                                             method='GET',
                                             xcom_push=True)

        clima_api_call = SimpleHttpOperator(task_id='clima_api_call',
                                             http_conn_id='previsao_do_tempo',
                                             endpoint='BR?token=1ec684d591206934b1ab8a634cd0cf3a',
                                             method='GET',
                                             xcom_push=True)


        load_in_postgres = PythonOperator(task_id='load_in_postgres',
                                          python_callable=pull_and_load,
                                          do_xcom_push=True,
                                          provide_context=True)



        select_from_table = PostgresOperator(task_id='select_10_from_table',
                                             sql="SELECT * FROM airlflow_dollar_clima LIMIT 10",
                                             postgres_conn_id='postgres_weatherdb',
                                             do_xcom_push=True,
                                             provide_context=True)

        #push_in = PythonOperator(task_id="pull_in_airflow",
                                   #python_callable=pull_in_airflow,
                                   #do_xcom_push=True,
                                   #provide_context=True)

        end = DummyOperator(task_id='end')

start >> [dollar_api_call,clima_api_call] >> load_in_postgres >> select_from_table  >> end