from defined_functions import MyFunctions
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from time import sleep
import pandas as pd
from faker import Faker
import random


def generate_fake_data(num_entries):
    fake = Faker()
    names = [fake.name() for _ in range(num_entries)]
    numbers = [[random.randint(1, 30), random.randint(1, 30), random.randint(1, 30)] for _ in range(num_entries)]
    return names, numbers

def log_into_csv():
    my_functions = MyFunctions()
    file_path = '/opt/airflow/data/output.csv'
    names, numbers = generate_fake_data(num_entries = 10)
    data = [["Name", "Sum_of_Numbers"]] 
    
    for name, numbers_set in zip(names, numbers):
        a, b, c = numbers_set
        result_greet = my_functions.greet(name)
        result_calculate_sum = my_functions.calculate_sum(a, b, c)
        data.append([result_greet, result_calculate_sum])

    my_functions.prepare_csv_data(file_path, data, column_separator = ",", row_separator = "\n")    
    print("Data with fake entries has been written to 'output.csv' in the data folder")

def ingest_into_db():
    while True:
        try:
            psql_engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
            break
        except OperationalError:
            sleep(0.1)
    print('Connection to PostgresSQL successful.')

    df_output = pd.read_csv("/opt/airflow/data/output.csv")
    df_output.head(n=0).to_sql(name = "functions_table", con=psql_engine, if_exists='replace', index=False)
    try:
        df_output.to_sql(name = 'functions_table', con=psql_engine, if_exists='append', index=False)
    except (OperationalError, ProgrammingError) as e:
        print('Error occured while inserting data in PostgresSQL {}'.format(e.args))
    print('Data inserted to PostgresSQL')

    try:
        df_pulled = pd.read_sql('SELECT * FROM functions_table', con=psql_engine)
    except (OperationalError, ProgrammingError) as e:
        print('Error occured while pulling data from PostgresSQL {}'.format(e.args))
    print('Data pulled From PostgresSQL')
    print(df_pulled.to_string())