from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date, to_timestamp

import random 
import pandas as pd


def get_some_data(): 

    start_date = date(2022, 1, 1)

    end_date = date(2022, 6, 30)
    record_list = []

    while start_date <= end_date:
        start_datetime = datetime(
            start_date.year, start_date.month, start_date.day, 
            random.randrange(6, 8), random.randrange(0, 59), random.randrange(0, 59)
        )
        start_kwh = 0.0
        record_list.append([start_datetime, start_kwh])
        for i in range(1, random.randrange(2, 15)):
            record_list.append(
                [
                    start_datetime+timedelta(minutes=i*45), 
                    round(i*random.uniform(1.10, 1.15), 2)
                ]
            )
        start_date = start_date + timedelta(1)
    
    df = pd.DataFrame(record_list)
    df.columns = ['time', 'produced_kwh']
    df.to_csv('/app/input_data/acid_example_data.txt', sep='\t', index=False)


def get_input_df(spark_session):
    return spark_session.read.option(
        'delimiter', '\t').option(
        'header', 'true').csv(
        'input_data/acid_example_data.txt'
    )


def get_happy_flow(df_input):
    return df_input.withColumn(
        "time", to_timestamp("time")).withColumn(
        "produced_kwh", col("produced_kwh").cast("decimal(18,2)")).withColumn(
        "reporting_date", to_date(col("time"),"yyyy-MM-dd")).withColumn(
        "reporting_month", date_format(col("reporting_date"),"yyyyMM"))


def get_error_flow(df_input):
    return df_input.withColumn(
        "produced_kwh", col("produced_kwh").cast("decimal(18,2)")).withColumn(
        "reporting_date", to_date(col("time"),"yyyy-MM-dd")).withColumn(
        "reporting_month", date_format(col("reporting_date"),"yyyyMM"))


def get_data(spark_session, flow_type):
    df_input = get_input_df(spark_session)
    if flow_type == 'error':
        df_input = get_error_flow(df_input)
    else:
        df_input = get_happy_flow(df_input)
    
    return df_input
