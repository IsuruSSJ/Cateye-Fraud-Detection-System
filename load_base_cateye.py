from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
import pandas as pd


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

users_path = r"/opt/airflow/datasets/users.csv"
cards_path = r"/opt/airflow/datasets/cards.csv"
merchants_path = r"/opt/airflow/datasets/merchants.csv"
transactions_path = r"/opt/airflow/datasets/transactions0-2.csv"


def read_csv_and_insert(file_path, table_name):
    df = pd.read_csv(file_path)
    df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
    insert_into_postgres(df, table_name)


def insert_into_postgres(df, table_name):
    # df = pd.read_json(json_data)
    postgres_hook = PostgresHook(postgres_conn_id='base_cateye')
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')

with DAG(
    dag_id='load_base_cateye',
    default_args=default_args,
    start_date=datetime(2021,1,1),
    schedule_interval='@once',
    catchup=False,
    tags=['Isuru', 'Cateye']
) as dag:

    with TaskGroup('creating_postgres_tables') as creating_postgres_tables:   
        create_users_table = PostgresOperator(
            task_id='create_users_table',
            postgres_conn_id='base_cateye',
            sql="""
                CREATE TABLE IF NOT EXISTS dim_users (
                    user_id INT PRIMARY KEY,
                    person VARCHAR(255),
                    gender VARCHAR(255),
                    age INT,
                    birth_year INT,
                    birth_month INT,
                    address VARCHAR(255),
                    apartment VARCHAR(255),
                    city VARCHAR(255),
                    state VARCHAR(255),
                    zipcode VARCHAR(255),
                    yearly_income FLOAT,
                    num_cards INT,
                    username VARCHAR(255), 
                    password VARCHAR(255)
                );
            """
        )

        create_cards_table = PostgresOperator(
            task_id='create_cards_table',
            postgres_conn_id='base_cateye',
            sql="""
                CREATE TABLE IF NOT EXISTS dim_cards (
                card_id INT PRIMARY KEY,
                user_id INT,
                card_index INT,
                brand VARCHAR(255),
                card_type VARCHAR(255),
                card_number VARCHAR(255),
                expires DATE,
                cards_issued INT,
                "limit" INT,
                open_date DATE,
                year_last_pin_change INT,
                FOREIGN KEY (user_id) REFERENCES dim_users(user_id)
            );
        """
        )

        create_merchants_table = PostgresOperator(
            task_id='create_merchants_table',
            postgres_conn_id='base_cateye',
            sql="""
                CREATE TABLE IF NOT EXISTS dim_merchants (
                merchant_id SERIAL PRIMARY KEY,
                merchant_name VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                zip_code VARCHAR(255)
            );
            """
        )

        create_transactions_table = PostgresOperator(
            task_id='create_transactions_table',
            postgres_conn_id='base_cateye',
            sql="""
                CREATE TABLE IF NOT EXISTS fact_transactions (
                fact_id SERIAL PRIMARY KEY,
                card_id INT,
                merchant_id INT,
                date_time TIMESTAMP,
                amount FLOAT,
                use_chip VARCHAR(255),
                mcc INT,
                card_on_dark_web BOOLEAN, 
                "error" VARCHAR(255),
                same_location BOOLEAN,
                time_difference INT,
                "ML_result" FLOAT,
                upload_date DATE,
                FOREIGN KEY (card_id) REFERENCES dim_cards(card_id),
                FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id)
            );
            """
        )

        create_users_table >> create_cards_table >> create_transactions_table
        create_merchants_table >> create_transactions_table     

    with TaskGroup('extract_and_load_data') as extract_and_load_data:
        extract_and_insert_users = PythonOperator(
            task_id='extract_and_insert_users',
            python_callable=read_csv_and_insert,
            op_kwargs={'file_path': users_path, 'table_name': 'dim_users'}
        )
        
        extract_and_insert_cards = PythonOperator(
            task_id='extract_and_insert_cards',
            python_callable=read_csv_and_insert,
            op_kwargs={'file_path': cards_path, 'table_name': 'dim_cards'}
        )
        
        extract_and_insert_merchants = PythonOperator(
            task_id='extract_and_insert_merchants',
            python_callable=read_csv_and_insert,
            op_kwargs={'file_path': merchants_path, 'table_name': 'dim_merchants'}
        )
        
        extract_and_insert_transactions = PythonOperator(
            task_id='extract_and_insert_transactions',
            python_callable=read_csv_and_insert,
            op_kwargs={'file_path': transactions_path, 'table_name': 'fact_transactions'}
        )
        
        [extract_and_insert_users, extract_and_insert_cards, extract_and_insert_merchants, extract_and_insert_transactions]


    creating_postgres_tables >> extract_and_load_data