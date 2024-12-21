from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os
import shutil
import logging

def validate_input_files():
    base_path = '/home/thoriqq/airflow/dags/final_project/'
    potential_buzzers_file = base_path + 'potential_buzzers.csv'
    user_activity_sorted_file = base_path + 'user_activity_sorted.csv'
    
    if not os.path.exists(potential_buzzers_file):
        raise FileNotFoundError(f"{potential_buzzers_file} does not exist.")
    if not os.path.exists(user_activity_sorted_file):
        raise FileNotFoundError(f"{user_activity_sorted_file} does not exist.")
    
    logging.info("Input files validated successfully.")

def run_spark_job():
    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("Join Buzzer Data") \
        .getOrCreate()

    # Path ke file CSV yang dihasilkan
    base_path = '/home/thoriqq/airflow/dags/final_project/'
    potential_buzzers_file = base_path + 'potential_buzzers.csv'
    user_activity_sorted_file = base_path + 'user_activity_sorted.csv'

    # Load data ke DataFrame PySpark
    df_buzzers = spark.read.csv(potential_buzzers_file, header=True, inferSchema=True)
    df_user_activity_sorted = spark.read.csv(user_activity_sorted_file, header=True, inferSchema=True)
 
    # Rename columns that may cause conflicts before performing join
    df_buzzers_renamed = df_buzzers \
        .withColumnRenamed("reply_count", "reply_count_buzzers") \
        .withColumnRenamed("retweet_count", "retweet_count_buzzers") \
        .withColumnRenamed("engagement", "engagement_buzzers") \
        .withColumnRenamed("engagement_rate", "engagement_rate_buzzers") \
        .withColumnRenamed("tweet_count", "tweet_count_buzzers")

    df_user_activity_sorted_renamed = df_user_activity_sorted \
        .withColumnRenamed("engagement", "engagement_user_activity") \
        .withColumnRenamed("engagement_rate", "engagement_rate_user_activity") \
        .withColumnRenamed("tweet_count", "tweet_count_user_activity")

    # 2. Gabungkan df_buzzers dengan df_user_activity_sorted berdasarkan 'username'
    df_buzzers_user_activity = df_buzzers_renamed.join(df_user_activity_sorted_renamed, on='username', how='inner')

    # Fungsi untuk menyimpan dengan nama file spesifik
    def save_as_single_csv(df, output_path, final_filename):
        temp_path = output_path + "_temp"
        # Simpan dalam satu partisi
        df.coalesce(1).write.csv(temp_path, header=True, mode="overwrite")
        # Cari file part-xxxx.csv dan rename
        for file in os.listdir(temp_path):
            if file.startswith("part-") and file.endswith(".csv"):
                shutil.move(os.path.join(temp_path, file), final_filename)
                break
        # Hapus folder sementara
        shutil.rmtree(temp_path)

    # Simpan hasil join ke CSV dengan nama spesifik
    save_as_single_csv(df_buzzers_user_activity, base_path + 'buzzers_user_activity', base_path + 'buzzers_user_activity.csv')

    # Tampilkan hasil join
    df_buzzers_user_activity.show()

    # Menutup SparkSession
    spark.stop()

# ============================
# DAG CONFIGURATION
# ============================
default_args = {
    'owner': 'Thoriq',
    'start_date': datetime(2024, 6, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spark_airflow_pipeline',
    default_args=default_args,
    description='Run PySpark Jobs in Airflow',
    schedule_interval=None,
    catchup=False
)

# ============================
# TASK DEFINITION
# ============================

validate_task = PythonOperator(
    task_id='validate_input_files',
    python_callable=validate_input_files,
    dag=dag
)

run_spark_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag
)

# ============================
# TASK SEQUENCE
# ============================
validate_task >> run_spark_task