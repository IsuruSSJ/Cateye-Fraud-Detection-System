from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
import pandas as pd
import os
import joblib
import shutil



# Define file paths
new_transactions_folder = r"/opt/airflow/cateye_files/new_transactions"
extracted_data_folder = r"/opt/airflow/cateye_files/extracted_data"
transformed_data_folder = r"/opt/airflow/cateye_files/transformed_data"
processed_transactions_folder = r"/opt/airflow/cateye_files/extracted_data"

extracted_output_file_paths = {
    'transactions': r"/opt/airflow/cateye_files/extracted_data/new_transactions_combined.csv",
    'cards': r"/opt/airflow/cateye_files/extracted_data/cards.csv",
    'users': r"/opt/airflow/cateye_files/extracted_data/users.csv",
    'merchants': r"/opt/airflow/cateye_files/extracted_data/merchants.csv"
}

# Airflow connection ID
connection_id = 'base_cateye'  

### DEFINE FUNCTIONS TO BE USED IN TRANSFORMATIONS
def convert_to_24_hour(time_str):
    datetime_obj = datetime.strptime(time_str, "%d/%m/%Y %I:%M:%S %p")
    return datetime_obj.strftime("%H:%M:%S")

def fixZIP(brokenZIP):
    fixedZIP = str(brokenZIP).zfill(5)
    return fixedZIP 

def amount(amount):
    if isinstance(amount, str) and ',' in amount:
        amount = amount.replace(',', '')
    if '$' in amount:
        amount = amount.replace('$', '')
        return float(amount)
    else:
        return amount
    
def mark_overseas(state, states):
    if state["Merchant State"] == "ONLINE":
        state["Zip"] = "ONLINE"    
    elif state["Merchant State"] not in states:
        state["Zip"] = "OVERSEAS"
    return state

def state_online(df):
    df.loc[df["Merchant City"] == "ONLINE", "Merchant State"] = "ONLINE"
    return df

def zip_online(df):
    df.loc[df["Merchant City"] == "ONLINE", "Zip"] = "ONLINE"
    return df

def fill_errors(row):
    if pd.isna(row["Errors?"]) or row["Errors?"] == "":
        row["Errors?"] = "No Error"
    return row

def remove_decimal_zero(num_str):
    num_str = str(num_str)
    if num_str.endswith('.0'):
        return num_str[:-2] 
    else:
        return num_str
    
def mysql_date(date_str):
    datetime_obj = datetime.strptime(date_str, "%b-%y")
    return datetime_obj.strftime("%Y-%m-%d")

def convert_yes_no_to_int(value):
    if value.strip().lower() == 'yes':
        return 1
    elif value.strip().lower() == 'no':
        return 0
    else:
        return None 

# USA states list
states = [
    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#States.
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
    "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI",
    "WV", "WY",
    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#Federal_district.
    "DC",
    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#Inhabited_territories.
    "AS", "GU", "MP", "PR", "VI",
]

### DEFINE FUNCTIONS FOR TASKS

# Function to extract new transactions and all user, card and merchant data from database
def cateye_extract(connection_id, new_transactions_folder, output_file_paths):

    print("Starting Extract")

    # Define SQL queries
    merchants_query = 'SELECT * FROM "dim_merchants"'
    users_query = 'SELECT * FROM "dim_users"'
    cards_query = 'SELECT * FROM "dim_cards"'

    # Create connection to postgres
    postgres_hook = PostgresHook(postgres_conn_id=connection_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # SQL queries to dataframes
    cards = pd.read_sql(cards_query, con=engine)
    users = pd.read_sql(users_query, con=engine)
    merchants = pd.read_sql(merchants_query, con=engine)

    # Create empty list to store data from each csv file
    new_transactions_list = []

    for file_name in os.listdir(new_transactions_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(new_transactions_folder, file_name)
            chunk = pd.read_csv(file_path)
            new_transactions_list.append(chunk)

    if len(new_transactions_list) == 0:
            print('DataFrame is empty!')
            return "No new transactions!"

    # Combine all chunks into a single dataframe
    new_transactions = pd.concat(new_transactions_list, ignore_index=True)

    # Save the dataframes to a CSV file
    new_transactions.to_csv(output_file_paths['transactions'], index=False)
    cards.to_csv(output_file_paths['cards'], index=True)
    users.to_csv(output_file_paths['users'], index=True)
    merchants.to_csv(output_file_paths['merchants'], index=True)

    print("Ending Extract")

    return "There are new transactions!"


# Function to transform the data (part 1)
def cateye_transform1():

    print("Starting Transform 1")

    # Define file paths
    transactions_file_path = r"/opt/airflow/cateye_files/extracted_data/new_transactions_combined.csv"
    output_file_path = r"/opt/airflow/cateye_files/transformed_data/transformed_transactions1.csv"
    
    # Read the CSV file into a pandas dataframe
    print("is df empty?")

    new_transactions = pd.read_csv(transactions_file_path)

    transactions = new_transactions
    
    if transactions.empty:
        print('DataFrame is empty!')
        return None
        
    # Convert amount to a float
    transactions['Amount'] = transactions['Amount'].apply(amount)

    # Convert Zip and Merchant name to string
    transactions['Zip'] = transactions['Zip'].astype(str)
    transactions['Merchant Name'] = transactions['Merchant Name'].astype(str)

    # Mark ONLINE transactions
    transactions = state_online(transactions)
    transactions = zip_online(transactions)

    # Mark OVERSEAS transactions
    transactions = transactions.apply(lambda row: mark_overseas(row, states), axis = 1)

    # Remove decimals in zip
    transactions['Zip'] = transactions['Zip'].apply(remove_decimal_zero)

    # Mark Error transactions
    transactions = transactions.apply(fill_errors, axis = 1)

    # Add ML_result column
    transactions.insert(1, "ML_result", 0.0, True)
    transactions['ML_result'] = transactions['ML_result'].astype(float)

    # Add upload_date column
    x = datetime.today().strftime('%Y-%m-%d')
    transactions.insert(1, "upload_date",x, True)

    # Add card_on_dark_web column
    transactions.insert(15, "card_on_dark_web",False, True)

    # Date column
    date_series = pd.to_datetime(transactions[['Year', 'Month', 'Day']])
    transactions.insert(2, "Date","", True)
    transactions['Date'] = date_series

    # Date_Time Column
    transactions['Date_Time'] = pd.to_datetime(transactions['Date'].astype(str) + ' ' + transactions['Time'].astype(str))

    # Drop Date and Time columns
    transactions = transactions.drop(columns=['Date', 'Time', 'Year', 'Month', 'Day', 'Is Fraud?'])  

    # Add Same location column
    transactions['Same Location'] = False

    # Iterate over each row in the dataframe
    for i in range(1, len(transactions)):
        # Check if it's the same user as the previous row
        if transactions.loc[i, 'User'] == transactions.loc[i - 1, 'User']:
            # Check if the merchant state is the same as the previous row
            transactions.loc[i, 'Same Location'] = transactions.loc[i, 'Merchant State'] == transactions.loc[i - 1, 'Merchant State']
        else:
            # If it's a different user, set 'Same Location' to False
            transactions.loc[i, 'Same Location'] = False

    # Save the dataframe to a CSV file
    transactions.to_csv(output_file_path, index=False)


# Function to transform the data (part 2)
def cateye_transform2():

    # Define file paths
    file_path_users = r"/opt/airflow/cateye_files/extracted_data/users.csv"
    file_path_cards = r"/opt/airflow/cateye_files/extracted_data/cards.csv"
    file_path_merchants = r"/opt/airflow/cateye_files/extracted_data/merchants.csv"
    transactions1_file_path = r"/opt/airflow/cateye_files/transformed_data/transformed_transactions1.csv"
    output_file_path = r"/opt/airflow/cateye_files/transformed_data/transformed_transactions2.csv"
    merchants_output = r"/opt/airflow/cateye_files/transformed_data/transformed_merchants.csv"

    transactions1 = pd.read_csv(transactions1_file_path)

    print("Starting Transform 2")

    users = pd.read_csv(file_path_users)
    users.drop(users.columns[users.columns.str.contains(
        'unnamed', case=False)], axis=1, inplace=True)
    cards = pd.read_csv(file_path_cards)
    cards.drop(cards.columns[cards.columns.str.contains(
        'unnamed', case=False)], axis=1, inplace=True)
    merchants = pd.read_csv(file_path_merchants)
    merchants.drop(merchants.columns[merchants.columns.str.contains(
        'unnamed', case=False)], axis=1, inplace=True)

    merchants.drop(merchants.columns[merchants.columns.str.contains(
        'merchant_id', case=False)], axis=1, inplace=True)

    transactions = transactions1

    if transactions.empty:
        print('DataFrame is empty!')
        return None

    # Add card_id
    transactions.rename(columns={"User": "user_id"}, inplace=True)
    transactions.rename(columns={"Card": "card_index"}, inplace=True)
    transactions = pd.merge(transactions,
                    cards[['card_id', 'user_id', 'card_index']],
                    on=['user_id', 'card_index'], how='left')
    
    # Drop the original columns 
    transactions.drop(['card_index'], axis=1, inplace=True)

    # Fixing merchants
    merchants_columns = ['Merchant Name', 'Merchant City', 'Merchant State', 'Zip']
    new_merchants = transactions[merchants_columns].drop_duplicates()
    new_merchants.rename(columns={
        "Merchant Name": "merchant_name",
        "Merchant City": "city",
        "Merchant State": "state",
        "Zip": "zip_code"
    }, inplace=True)

    new_merchants.drop_duplicates(subset=['merchant_name', 'city', 'state', 'zip_code'])

    added_merchants = pd.concat([merchants, new_merchants]).drop_duplicates(keep=False)

    # Assign new merchant_ids
    postgres_hook = PostgresHook(postgres_conn_id='base_cateye')
    engine = postgres_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        result = conn.execute("SELECT COALESCE(MAX(merchant_id), 0) FROM dim_merchants;")
        max_merchant_id = result.scalar()

    added_merchants['merchant_id'] = range(max_merchant_id + 1, max_merchant_id + 1 + len(added_merchants))

    # Load updated merchants table
    added_merchants.to_csv(merchants_output, index=False)

    merchants = pd.read_csv(file_path_merchants)
    merchants.drop(merchants.columns[merchants.columns.str.contains(
        'unnamed', case=False)], axis=1, inplace=True)
    full_merchants = pd.concat([merchants, added_merchants], ignore_index=True)

    # Merge merchant_id
    transactions.rename(columns={
        "Merchant Name": "merchant_name",
        "Merchant City": "city",
        "Merchant State": "state",
        "Zip": "zip_code"
    }, inplace=True)
    transactions = pd.merge(transactions,
        full_merchants[['merchant_id', 'merchant_name', 'city', 'state', 'zip_code']],
        on=['merchant_name', 'city', 'state', 'zip_code'], how='left')
    
    # Drop the original columns 
    transactions.drop(['merchant_name', 'city', 'state', 'zip_code'], axis=1, inplace=True)

    # Ensure Date_Time is in datetime format
    transactions['Date_Time'] = pd.to_datetime(transactions['Date_Time'], errors='coerce')

    # Add Time since Last Transaction column
    transactions['Minutes since last Purchase'] = transactions.groupby('user_id')['Date_Time'].diff().dt.total_seconds() / 60
    transactions['Minutes since last Purchase'] = transactions['Minutes since last Purchase'].fillna(0)
    transactions['Minutes since last Purchase'] = transactions['Minutes since last Purchase'].astype(int)

    # Drop user_id
    transactions = transactions.drop(columns=['user_id'])

    print("Columns in transactions DataFrame:", transactions.columns)

    # Rename columns for postgres
    transactions.rename(columns={"Amount": "amount", "Use Chip": "use_chip", "MCC": "mcc", 
                                 "Date_Time": "date_time", "Errors?": "error", 
                                 "Same Location": "same_location", "Minutes since last Purchase": "time_difference"}, 
                                 inplace=True)
    
    print("Columns in transactions DataFrame:", transactions.columns)

    # Assign new fact_ids
    with engine.connect() as conn:
        result = conn.execute("SELECT COALESCE(MAX(fact_id), 0) FROM fact_transactions;")
        max_fact_id = result.scalar()

    
    transactions['fact_id'] = range(max_fact_id + 1, max_fact_id + 1 + len(transactions))

    # Change order of columns
    transactions = transactions[['fact_id', 'card_id', 'merchant_id', 'date_time', 'amount', 'use_chip', 'mcc', 'card_on_dark_web', 'error', 'same_location', 'time_difference', 'ML_result', 'upload_date']]
    print("Columns in transactions DataFrame:", transactions.columns)

    # Save the dataframe to a CSV file
    transactions.to_csv(output_file_path, index=False)


# Function to load the data (part 1)
def cateye_load1():

    print("Starting Load 1")

    # Find all new merchants in this upload to add to dim_merchant
    new_merchants_file_path = r"/opt/airflow/cateye_files/transformed_data/transformed_merchants.csv"
    new_merchants = pd.read_csv(new_merchants_file_path)
    postgres_hook = PostgresHook(postgres_conn_id='base_cateye')
    engine = postgres_hook.get_sqlalchemy_engine()

    if new_merchants.empty:
        print('DataFrame is empty!')
        return None
        
    print("Starting process for SQL --- merchants")

    print(new_merchants)

    new_merchants.to_sql(name='dim_merchants', con=engine, if_exists='append', index=False, method='multi')

    print("Done SQL --- merchants")
     

# Function to load the data (part 2)
def cateye_load2():

    print("Starting Load 2")
    transactions2_file_path = r"/opt/airflow/cateye_files/transformed_data/transformed_transactions2.csv"

    transactions2 = pd.read_csv(transactions2_file_path)

    print(transactions2.info()) 

    if transactions2.empty:
        print('DataFrame is empty!')
        return None
    
    print("Starting process for SQL --- transactions")

    postgres_hook = PostgresHook(postgres_conn_id='base_cateye')
    engine = postgres_hook.get_sqlalchemy_engine()

    transactions2.to_sql(name='fact_transactions', con=engine, if_exists='append', index=False, method='multi')

    print("Done SQL --- transactions")


# Function to apply the Logistic Regression Model to the new transactions
def cateye_ML():

    # Define paths and connections
    postgres_hook = PostgresHook(postgres_conn_id='base_cateye')
    engine = postgres_hook.get_sqlalchemy_engine()
    model_path = r"/opt/airflow/cateye_files/cateye_model.pkl" 
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Make sure only new transactions get put through the ML model
    x = datetime.today().strftime('%Y-%m-%d')
    
    ML_query = """
        SELECT
            f.fact_id,
            f.amount,
            f.use_chip,
            f.time_difference,
            m.state,
            u.age,
            c.card_type,
            c.year_last_pin_change,
            f."ML_result"
        FROM 
            "fact_transactions" f
            INNER JOIN "dim_merchants" m
                ON m.merchant_id = f.merchant_id
            INNER JOIN "dim_cards" c
                ON c.card_id = f.card_id
            INNER JOIN "dim_users" u
                ON u.user_id = c.user_id
        WHERE
            upload_date = %s
        ;
    """

    new_transactions = pd.read_sql(ML_query, con=engine, params=(x,))
    new_transactions = pd.get_dummies(new_transactions, columns=['use_chip', 'state', 'card_type'])
    model, ref_cols, target = joblib.load(model_path)

    missing_cols = [col for col in ref_cols if col not in new_transactions.columns]

    # Add missing columns with 0 values
    if missing_cols:
        missing_data = pd.DataFrame(0, index=new_transactions.index, columns=missing_cols)
        new_transactions = pd.concat([new_transactions, missing_data], axis=1)

    # Ensure columns are in the same order as ref_cols
    new_transactions_id = new_transactions['fact_id']
    new_transactions_ML = new_transactions['ML_result']
    new_transactions = new_transactions[ref_cols]

    # Check and align target column
    if target not in new_transactions.columns:
        new_transactions[target] = 0

    X_new = new_transactions[ref_cols]

    print("Starting ML")

    if X_new.empty:
        print('DataFrame is empty!')
        return None
    
    probabilities = model.predict_proba(X_new)[:, 1]

    upload_data = pd.DataFrame(new_transactions_id)
    upload_data.insert(1, "ML_result", probabilities, True)

    print("Starting process for SQL --- transactions")

    # Update transactions with new ML results
    for index, row in upload_data.iterrows():
        sql = """
            UPDATE fact_transactions
            SET "ML_result" = %s
            WHERE fact_id = %s
        """
        values = (row['ML_result'], row['fact_id'])
        cursor.execute(sql, values)

    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()

    print("ML update complete")


# Function to reset for next dag run
def cateye_reset():  

    # Define folder paths
    new_transactions_folder = r"/opt/airflow/cateye_files/new_transactions"
    processed_data_folder = r"/opt/airflow/cateye_files/processed_transactions"
    extracted_data_folder = r"/opt/airflow/cateye_files/extracted_data"
    transformed_data_folder = r"/opt/airflow/cateye_files/transformed_data"

    print("Starting Reset")

    # Move raw new transaction data to a separate folder
    files = os.listdir(new_transactions_folder)   
    for file in files:
        shutil.move(os.path.join(new_transactions_folder, file), processed_data_folder)

    # Reset the extracted_data and transformed_data folders
    shutil.rmtree(extracted_data_folder)
    os.mkdir(extracted_data_folder) 
    shutil.rmtree(transformed_data_folder)
    os.mkdir(transformed_data_folder) 

    print("Ending ETL")


# Function to branch the DAG depending on if there is new data or not
def is_new_data(ti):
    new_data = ti.xcom_pull(task_ids='extract')
    print(new_data)
    if new_data == "No new transactions!":
        return 'no_new_data_found'
    else:
        return 'new_data_found'
    



### DAG 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}





with DAG(
    dag_id='cateye_etl_dag',
    default_args=default_args,
    start_date=datetime(2021,1,1),
    schedule_interval='@once',
    catchup=False,
    tags=['Isuru', 'Cateye']
) as dag:
    with TaskGroup('extracting') as extracting:
        extract = PythonOperator(
            task_id='extract',
            python_callable=cateye_extract,
            op_kwargs={
            'connection_id': connection_id,
            'new_transactions_folder': new_transactions_folder,
            'output_file_paths': extracted_output_file_paths
        }
        )
    
    with TaskGroup('checking_if_new_data_exists') as checking_if_new_data_exists:
        new_data = BranchPythonOperator(
            task_id='is_new_data',
            python_callable=is_new_data
        )

        no_new_data_found = BashOperator(
            task_id='no_new_data_found',
            bash_command="echo 'There are no new transactions!'"
        )

        new_data_found = BashOperator(
            task_id='new_data_found',
            bash_command="echo 'There are new transactions!'"
        )

        new_data >> [no_new_data_found, new_data_found]

    with TaskGroup('transforming') as transforming:
        transform_1 = PythonOperator(
            task_id='transform_1',
            python_callable=cateye_transform1
        )

        transform_2 = PythonOperator(
            task_id='transform_2',
            python_callable=cateye_transform2
        )

        transform_1 >> transform_2

    with TaskGroup('loading') as loading:
        load_1 = PythonOperator(
            task_id='load_1',
            python_callable=cateye_load1
        )

        load_2 = PythonOperator(
            task_id='load_2',
            python_callable=cateye_load2
        )

        load_1 >> load_2

    apply_ML = PythonOperator(
        task_id='apply_ML',
        python_callable=cateye_ML
    )

    reset = PythonOperator(
        task_id='reset',
        python_callable=cateye_reset
    )


    extracting >> checking_if_new_data_exists
    new_data_found >> transforming >> loading >> apply_ML >> reset