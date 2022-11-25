import os
import pandas as pd
import psycopg2
from pyspark.sql.session import SparkSession
from sqlalchemy import create_engine

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local").appName("Get Data Transaction From Database").getOrCreate()
    

    # connection
    url = 'postgresql+psycopg2://root:root@localhost:5432/source_data'
    engine = create_engine(url=url)

    df = pd.read_sql(sql= 'SELECT * FROM bigdata_transaction', con=engine)

    spark_df = spark_session.createDataFrame(df)
    spark_df.show()
    
