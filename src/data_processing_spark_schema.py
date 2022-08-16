from uuid import uuid4
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, DateType, TimestampType, DoubleType

import random 


def get_spark_session():
    """
    Returns:
        SparkSession: spark session with the spark-excel package
    """
    return SparkSession.builder.appName("My Spark ETL Session").config(
    'spark.hive.metastore.uris', 'thrift://localhost:9083'
    ).enableHiveSupport().getOrCreate()


def get_some_df(spark_session): 

    start_date = date(2022, 6, 29)

    end_date = date(2022, 7, 5)
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
    
    schema = StructType(
        [
            StructField('time',TimestampType(),True),
            StructField('produced_kwh',DoubleType(),True)
        ]
    )

    return spark_session.createDataFrame(record_list, schema)


def spark_overwrite(spark_session, table_name, input_df):
    spark_session.sql(f"drop table if exists {table_name}")
    input_df.write.saveAsTable(table_name)


def save_raw_data(spark_session):
    input_df = get_some_df(spark_session)
    spark_overwrite(
        spark_session=spark_session, 
        table_name='raw_electricity_data',
        input_df=input_df
    )


def save_agg_data(spark_session): 
    agg_df = spark_session.sql(
        "select to_date(time, 'yyyy-MM-dd') as reporting_date, \
            max(produced_kwh) produced_kwh \
        from raw_electricity_data group by 1"
    )
    spark_overwrite(
        spark_session=spark_session, 
        table_name='daily_electricity_agg',
        input_df=agg_df
    )


def save_altered_agg_data(spark_session):
    altered_agg_df = spark_session.sql(
        "select reporting_date, \
            produced_kwh, \
            (max_unix_timestamp - min_unix_timestamp)/60/60 as no_active_hours \
        from (select to_date(time, 'yyyy-MM-dd') as reporting_date, \
            max(produced_kwh) produced_kwh, \
            min(unix_timestamp(time)) as min_unix_timestamp, \
            max(unix_timestamp(time)) as max_unix_timestamp \
        from raw_electricity_data group by 1) src"
    )
    spark_overwrite(
        spark_session=spark_session, 
        table_name='altered_daily_electricity_agg',
        input_df=altered_agg_df
    )


def create_view(spark_session):
    spark_session.sql(
        "create or replace view daily_electricity_agg_v as \
        select reporting_date, produced_kwh, null as no_active_hours from daily_electricity_agg \
        union all \
        select reporting_date, produced_kwh, no_active_hours from altered_daily_electricity_agg"
    )


if __name__ == "__main__":
    spark_session = get_spark_session()
    spark_session.sparkContext.setLogLevel('ERROR')
    print("Loading raw data...")
    save_raw_data(spark_session)
    print("Loading agg data...")
    save_agg_data(spark_session)
    print("Loading altered agg data...")
    save_altered_agg_data(spark_session)
    print("Creating view...")
    create_view(spark_session)
    print(spark_session.sql('select * from daily_electricity_agg_v').show())
