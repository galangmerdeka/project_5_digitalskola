import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

from mrjob.job import MRJob
from mrjob.step import MRStep

cols = 'id_transaction, id_customer, date_transaction, product_transaction, amount_transaction'.split(',')

def read_data_db():
    # connection
    url = 'postgresql+psycopg2://root:root@localhost:5432/source_data'
    engine = create_engine(url=url)

    # read data
    df = pd.read_sql_table(table_name='bigdata_transaction', con=engine)
    # data = list(df.itertuples(index=False))
    data = [tuple(x) for x in df.values]
    for row in data:
        return row

    # return df.head(10)

class OrderMonthCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                    mapper=self.mapper,
                    reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]       

    def mapper(self, _, line):
        # convert each line into dictionary
        row = dict(zip(cols, read_data_db(line)))

        yield row['date_transaction'][5:7], 1

    def reducer(self, key, values):
        yield None, (key, sum(values)) 

    def sort(self, key, values):
        data = []
        for order_month, order_count in values:
            data.append((order_month, order_count))
            data.sort()
        
        for order_month, order_count in data:
            yield order_month, order_count
    
    def mapper_final(self):
        self.conn.commit()
        self.conn.close()

if __name__ == "__main__":
    OrderMonthCount.run()