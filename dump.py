import pandas as pd
import numpy as np
import os
import psycopg2
from sqlalchemy import create_engine

if __name__ == "__main__":
    # print(f'Hello World')
    path = os.getcwd() + '/data/'
    # print(path)
    list_csv_file = [
        'bigdata_customer',
        'bigdata_product',
        'bigdata_transaction'
    ]

    for file in list_csv_file:
        df = pd.read_csv(path + file + '.csv')
        # print(df.head(5))

        # connection
        url = 'postgresql+psycopg2://root:root@localhost:5432/source_data'
        engine = create_engine(url=url)

        try:
            print(f'Dump {file} is Processing....')
            # dump data
            df.to_sql(name=file, index=False, con=engine, if_exists='replace')
            print(f'Dump {file} to Database Success')
        except:
            print(f'Dump {file} to Database Failed')