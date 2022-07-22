from uuid import uuid4
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, DateType, TimestampType, DoubleType


def get_spark_session():
    """
    Returns:
        SparkSession: spark session with the spark-excel package
    """
    return SparkSession.builder.appName("My Spark ETL Session").config(
    'spark.hive.metastore.uris', 'thrift://localhost:9083'
    ).enableHiveSupport().getOrCreate()


def get_some_df(spark_session):

    records = [
        [date(2022, 5, 3), datetime(2022, 5, 3, 21, 36, 3), 4.7],
        [date(2022, 7, 14), datetime(2022, 7, 14, 22, 30, 6), 18.8],
        [date(2022, 7, 15), datetime(2022, 7, 15, 22, 27, 30), 21.4],
        [date(2022, 7, 16), datetime(2022, 7, 16, 22, 28, 36), 23.5],
        [date(2022, 7, 17), datetime(2022, 7, 17, 22, 26, 15), 22.1]
    ]
    schema = StructType(
        [
            StructField('reporting_date',DateType(),True),
            StructField('time',TimestampType(),True),
            StructField('produced_kwh',DoubleType(),True)
        ]
    )

    return spark_session.createDataFrame(records, schema)


if __name__ == "__main__":
    spark_session = get_spark_session()
    data_to_be_saved = get_some_df(spark_session)

    table_name = f"full_electricity_{uuid4().hex.upper()[:6]}"

    data_to_be_saved.write.saveAsTable(table_name)

    print(
        spark_session.sql(f"select * from {table_name}").count()
    )
