from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from cryptography.fernet import Fernet
import logging
import os

# ============================
# CONFIGURATION AND CONSTANTS
# ============================
BASE_PATH = '/home/thoriqq/airflow/dags/final_project/'
RAW_FILE = os.path.join(BASE_PATH, 'pemilu1_after_ingest.csv')
INGESTED_FILE = os.path.join(BASE_PATH, 'ingested_data.csv')
ENCRYPTED_FILE = os.path.join(BASE_PATH, 'encrypted_data.csv')
USER_ACTIVITY_FILE = os.path.join(BASE_PATH, 'user_activity_sorted.csv')
POTENTIAL_BUZZERS_FILE = os.path.join(BASE_PATH, 'potential_buzzers.csv')

# Fetch encryption key from Airflow Variables or generate one
ENCRYPTION_KEY = Variable.get("encryption_key", default_var=Fernet.generate_key().decode())
fernet = Fernet(ENCRYPTION_KEY.encode())

# ============================
# FUNCTIONS DEFINITIONS
# ============================

def ingest_data():
    """Reads raw CSV file and saves it as ingested data."""
    try:
        logging.info("Starting data ingestion...")
        dataset = pd.read_csv(RAW_FILE)
        dataset.to_csv(INGESTED_FILE, index=False)
        logging.info("Data ingestion completed.")
    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")
        raise

def encrypt_data():
    """Encrypt sensitive columns in ingested data."""
    try:
        logging.info("Starting data encryption...")
        dataset = pd.read_csv(INGESTED_FILE)
        sensitive_cols = ['conversation_id_str', 'id_str', 'user_id_str']
        for col in sensitive_cols:
            dataset[col] = dataset[col].apply(lambda x: fernet.encrypt(str(x).encode()).decode())
        dataset = dataset.applymap(lambda x: x.replace("'", '"') if isinstance(x, str) else x)
        dataset.to_csv(ENCRYPTED_FILE, index=False)
        logging.info("Data encryption completed.")
    except Exception as e:
        logging.error(f"Error during encryption: {str(e)}")
        raise

def find_buzzers():
    """Identify potential buzzers based on engagement and tweet count."""
    try:
        logging.info("Identifying buzzers...")

        # Load the ingested data
        dataset = pd.read_csv(INGESTED_FILE)

        # Convert created_at to datetime and extract hour for activity pattern analysis
        dataset['created_at'] = pd.to_datetime(dataset['created_at'])
        dataset['hour'] = dataset['created_at'].dt.hour

        # Grouping user activity by username and calculating various metrics
        user_activity = dataset.groupby('username').agg({
            'full_text': 'count',          # Tweet count
            'retweet_count': 'sum',        # Total retweets
            'reply_count': 'sum',          # Total replies
        }).rename(columns={'full_text': 'tweet_count'})

        # Calculate engagement (retweets + replies)
        user_activity['engagement'] = user_activity['retweet_count'] + user_activity['reply_count']
        user_activity['engagement_rate'] = (user_activity['retweet_count'] + user_activity['reply_count']) / user_activity['tweet_count']

        # Sorting users based on engagement and tweet count
        user_activity_sorted = user_activity.sort_values(by=['engagement', 'tweet_count'], ascending=[False, False])

        # Calculate engagement per retweet and per reply
        user_activity['retweet_engagement'] = user_activity['retweet_count'] / user_activity['tweet_count']
        user_activity['reply_engagement'] = user_activity['reply_count'] / user_activity['tweet_count']

        # Identify potential buzzers based on engagement (more than 3x mean engagement)
        potential_buzzers = user_activity[user_activity['engagement'] > 3 * user_activity['engagement'].mean()]

        # Saving the user activity sorted data and potential buzzers to CSV files
        user_activity_sorted.to_csv(USER_ACTIVITY_FILE, index=True)
        potential_buzzers.to_csv(POTENTIAL_BUZZERS_FILE, index=True)

        logging.info("Buzzer identification completed.")

        # Return potential buzzers for further use or analysis
        return potential_buzzers
    except Exception as e:
        logging.error(f"Error identifying buzzers: {str(e)}")
        raise

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
    'efficient_buzzer_pipeline',
    default_args=default_args,
    description='Efficient DAG to find buzzers and analyze data',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 11, 11),
    catchup=False
)

# ============================
# TASK DEFINITIONS
# ============================


ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

encrypt_task = PythonOperator(
    task_id='encrypt_data',
    python_callable=encrypt_data,
    dag=dag,
)

find_buzzers_task = PythonOperator(
    task_id='find_buzzers',
    python_callable=find_buzzers,
    dag=dag,
)

# ============================
# TASK DEPENDENCIES
# ============================
ingest_task >> encrypt_task >> find_buzzers_task