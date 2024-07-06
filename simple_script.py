import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlalchemy
import os
import logging

import data_processing

logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')
# set env file to .env
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("DB_INSERT_USERNAME")
password = os.getenv("DB_INSERT_USERNAME_PASSWORD")
hostname = os.getenv("DB_HOSTNAME")
database = os.getenv("DB_INSERT_DATABASE")
port = os.getenv("DB_PORT")

logging.info(f"Connecting to {hostname}:{port} with: {user} and {password}")
engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}:{port}/{database}', echo=False, pool_pre_ping=True, pool_recycle=3600)
logging.info("Connected to database")

data_processing.cleanup_processed_files()

# SELECT FROM TABLE INFORMATION_SCHEMA.TABLES ALL TABLES IN SCHEMA prun_data as list variable "tables" using salalchemy
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'prun_data'"
tables = pd.read_sql(query, engine)
tables_list = tables['table_name'].tolist()
tables_list.sort()

preffered_file_types = ['csv', 'parquet']
for file_type in preffered_file_types:
    if os.path.exists(f"./{file_type}"):
        pass
    else:
        os.mkdir(f"{file_type}")
logging.info(f"Reading tables: {tables_list}")
for table_name in tables_list:
    try:
        logging.info(f"Reading table: {table_name}")
        df = pd.DataFrame(pd.read_sql(f'SELECT * FROM prun_data."{table_name}";', engine))
        logging.info(f"Table {table_name} read")
        df.to_csv(f"./csv/{table_name}.csv", index=False)
        df.to_parquet(f"./parquet/{table_name}.parquet", index=False, engine='pyarrow')
    except Exception as e:
        logging.error(f"Error reading table: {table_name} with error: {e}")
        continue

data_processing.create_plots(["temporary_df_hold_bids.csv", "temporary_df_hold_orders.csv"],
                             ["H2O", "LST", "O", "FEO", "FE", "COF", "NS", "PT", "OVE"])