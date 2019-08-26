from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import create_tables

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ao',
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('create_table_dag',
          default_args=default_args,
          description='Drop and create tables in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_artists_table = PostgresOperator(
    task_id='drop_artists',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_artists_table
)

create_artists_table = PostgresOperator(
    task_id='create_songplas',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_artists_table
)
    
drop_songplays_table = PostgresOperator(
    task_id='drop_songplays',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_songplays_table
)

create_songplays_table = PostgresOperator(
    task_id='create_songplays',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_songplays_table
)
   
drop_songs_table = PostgresOperator(
    task_id='drop_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_songs_table
)

create_songs_table = PostgresOperator(
    task_id='create_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_songs_table
)   

drop_staging_events_table = PostgresOperator(
    task_id='drop_staging_events',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_staging_events_table
)

create_staging_events_table = PostgresOperator(
    task_id='create_staging_events',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_staging_events_table
)   
    
drop_staging_songs_table = PostgresOperator(
    task_id='drop_staging_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_staging_songs_table
)

create_staging_songs_table = PostgresOperator(
    task_id='create_staging_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_staging_songs_table
)   
    
drop_time_table = PostgresOperator(
    task_id='drop_time',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_time_table
)

create_time_table = PostgresOperator(
    task_id='create_time',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_time_table
)
    
drop_users_table = PostgresOperator(
    task_id='drop_users',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.drop_users_table
)

create_users_table = PostgresOperator(
    task_id='create_users',
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_users_table
)
    
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_artists_table
start_operator >> drop_songplays_table
start_operator >> drop_songs_table
start_operator >> drop_staging_events_table
start_operator >> drop_staging_songs_table
start_operator >> drop_time_table
start_operator >> drop_users_table
drop_artists_table >> create_artists_table
drop_songplays_table >> create_songplays_table
drop_songs_table >> create_songs_table
drop_staging_events_table >> create_staging_events_table
drop_staging_songs_table >> create_staging_songs_table
drop_time_table >> create_time_table
drop_users_table >> create_users_table
create_artists_table >> end_operator
create_songplays_table >> end_operator
create_songs_table >> end_operator
create_staging_events_table >> end_operator
create_staging_songs_table >> end_operator
create_time_table >> end_operator
create_users_table >> end_operator