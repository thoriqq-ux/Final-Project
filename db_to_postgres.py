from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# ============================
# CONFIGURATION AND CONSTANTS
# ============================
BASE_PATH = '/home/thoriqq/airflow/dags/final_project/'

# File paths
USER_ACTIVITY_FILE = os.path.join(BASE_PATH, 'user_activity_sorted.csv')
POTENTIAL_BUZZERS_FILE = os.path.join(BASE_PATH, 'potential_buzzers.csv')
BUZZERS_USER_ACTIVITY_FILE = os.path.join(BASE_PATH, 'buzzers_user_activity.csv')
ENCRYPTED_DATA_FILE = os.path.join(BASE_PATH, 'encrypted_data.csv')

# ============================
# FUNCTION DEFINITIONS
# ============================

def read_csv_to_xcom(file_path, ti):
    """Reads CSV file and pushes data to XCom."""
    df = pd.read_csv(file_path)
    data = df.to_dict(orient='records')
    ti.xcom_push(key='data', value=data)

# ============================
# DAG CONFIGURATION
# ============================
default_args = {
    'owner': 'Thoriq',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['rafifthoriq30@gmail.com'],
    'email_on_failure': True,
}

dag = DAG(
    'csv_to_postgres_pipeline',
    default_args=default_args,
    description='Read CSVs and insert data into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 6, 11),
    catchup=False
)

# ============================
# TASK DEFINITIONS
# ============================

# 1. Create Tables
create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_conn',
    sql="""
    CREATE TABLE IF NOT EXISTS user_activity (
        username VARCHAR PRIMARY KEY,
        tweet_count INT,
        retweet_count INT,
        reply_count INT,
        engagement INT,
        engagement_rate FLOAT
    );
    CREATE TABLE IF NOT EXISTS potential_buzzers (
        username VARCHAR PRIMARY KEY,
        tweet_count INT,
        retweet_count INT,
        reply_count INT,
        engagement INT,
        engagement_rate FLOAT,
        retweet_engagement FLOAT,
        reply_engagement FLOAT
    );
    CREATE TABLE IF NOT EXISTS buzzers_user_activity (
        username VARCHAR PRIMARY KEY,
        tweet_count_buzzers INT,
        retweet_count_buzzers INT,
        reply_count_buzzers INT,
        engagement_buzzers INT,
        engagement_rate_buzzers FLOAT,
        retweet_engagement FLOAT,
        reply_engagement FLOAT,
        tweet_count_user_activity INT,
        retweet_count INT,
        reply_count INT,
        engagement_user_activity INT,
        engagement_rate_user_activity FLOAT
    );
        CREATE TABLE IF NOT EXISTS encrypted_data (
        conversation_id_str VARCHAR PRIMARY KEY,
        created_at TIMESTAMP,
        favorite_count INT,
        full_text TEXT,
        id_str VARCHAR,
        lang VARCHAR,
        quote_count INT,
        reply_count INT,
        retweet_count INT,
        tweet_url VARCHAR,
        user_id_str VARCHAR,
        username VARCHAR,
        IN_REPLY_TO_SCREEN_NAME_1 VARCHAR,
        LOCATION_1 VARCHAR
    );
    """,
    dag=dag
)

# 2. Read and Insert User Activity
read_user_activity = PythonOperator(
    task_id='read_user_activity',
    python_callable=read_csv_to_xcom,
    op_kwargs={'file_path': USER_ACTIVITY_FILE},
    dag=dag
)

insert_user_activity = PostgresOperator(
    task_id='insert_user_activity',
    postgres_conn_id='postgres_conn',
    sql="""
    INSERT INTO user_activity (username, tweet_count, retweet_count, reply_count, engagement, engagement_rate)
    VALUES 
    {% for record in ti.xcom_pull(task_ids='read_user_activity', key='data') %}
        ('{{ record.username }}', {{ record.tweet_count }}, {{ record.retweet_count }}, {{ record.reply_count }},
         {{ record.engagement }}, {{ record.engagement_rate }})
        {% if not loop.last %},{% endif %}
    {% endfor %};
    """,
    dag=dag
)

# 3. Read and Insert Potential Buzzers
read_potential_buzzers = PythonOperator(
    task_id='read_potential_buzzers',
    python_callable=read_csv_to_xcom,
    op_kwargs={'file_path': POTENTIAL_BUZZERS_FILE},
    dag=dag
)

insert_potential_buzzers = PostgresOperator(
    task_id='insert_potential_buzzers',
    postgres_conn_id='postgres_conn',
    sql="""
    INSERT INTO potential_buzzers (username, tweet_count, retweet_count, reply_count, engagement, engagement_rate, retweet_engagement, reply_engagement)
    VALUES 
    {% for record in ti.xcom_pull(task_ids='read_potential_buzzers', key='data') %}
        ('{{ record.username }}', {{ record.tweet_count }}, {{ record.retweet_count }}, {{ record.reply_count }}, {{ record.engagement }}, {{ record.engagement_rate }}, {{ record.retweet_engagement }}, {{ record.reply_engagement }})
        {% if not loop.last %},{% endif %}
    {% endfor %};
    """,
    dag=dag
)

# Task 3: Read and Insert Buzzers User Activity
read_buzzers_user_activity = PythonOperator(
    task_id='read_buzzers_user_activity',
    python_callable=read_csv_to_xcom,
    op_kwargs={'file_path': BUZZERS_USER_ACTIVITY_FILE},
    dag=dag
)

insert_buzzers_user_activity = PostgresOperator(
    task_id='insert_buzzers_user_activity',
    postgres_conn_id='postgres_conn',
    sql="""
    INSERT INTO buzzers_user_activity (username, tweet_count_buzzers, retweet_count_buzzers, reply_count_buzzers,
        engagement_buzzers, engagement_rate_buzzers, retweet_engagement, reply_engagement, 
        tweet_count_user_activity, retweet_count, reply_count, engagement_user_activity, engagement_rate_user_activity)
    VALUES 
    {% for record in ti.xcom_pull(task_ids='read_buzzers_user_activity', key='data') %}
        ('{{ record.username }}', {{ record.tweet_count_buzzers }}, {{ record.retweet_count_buzzers }},
         {{ record.reply_count_buzzers }}, {{ record.engagement_buzzers }}, {{ record.engagement_rate_buzzers }},
         {{ record.retweet_engagement }}, {{ record.reply_engagement }}, {{ record.tweet_count_user_activity }},
         {{ record.retweet_count }}, {{ record.reply_count }}, {{ record.engagement_user_activity }},
         {{ record.engagement_rate_user_activity }})
        {% if not loop.last %},{% endif %}
    {% endfor %};
    """,
    dag=dag
)

# 4. Read and Insert Encrypted Data
read_encrypted_data = PythonOperator(
    task_id='read_encrypted_data',
    python_callable=read_csv_to_xcom,
    op_kwargs={'file_path': ENCRYPTED_DATA_FILE},
    dag=dag
)

insert_encrypted_data = PostgresOperator(
    task_id='insert_encrypted_data',
    postgres_conn_id='postgres_conn',
    sql="""
    INSERT INTO encrypted_data (conversation_id_str, created_at, favorite_count, full_text, id_str, lang, 
        quote_count, reply_count, retweet_count, tweet_url, user_id_str, username, 
        IN_REPLY_TO_SCREEN_NAME_1, LOCATION_1)
    VALUES 
    {% for record in ti.xcom_pull(task_ids='read_encrypted_data', key='data') %}
        ('{{ record.conversation_id_str }}', '{{ record.created_at }}', {{ record.favorite_count }}, '{{ record.full_text }}', 
         '{{ record.id_str }}', '{{ record.lang }}', {{ record.quote_count }}, {{ record.reply_count }}, 
         {{ record.retweet_count }}, '{{ record.tweet_url }}', '{{ record.user_id_str }}', 
         '{{ record.username }}', '{{ record.IN_REPLY_TO_SCREEN_NAME_1 }}', '{{ record.LOCATION_1 }}')
        {% if not loop.last %},{% endif %}
    {% endfor %};
    """,
    dag=dag
)

# ============================
# TASK DEPENDENCIES
# ============================
create_tables_task >> [
    read_user_activity >> insert_user_activity,
    read_potential_buzzers >> insert_potential_buzzers,
    read_buzzers_user_activity >> insert_buzzers_user_activity,
    read_encrypted_data >> insert_encrypted_data
]
