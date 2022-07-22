from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, lit, col, split, regexp_replace, row_number, to_date
)
from pyspark.sql import Window
from data_processing_spark import get_raw_electricity_data, data_transformation
from uuid import uuid4

def get_spark_session():
    """
    Returns:
        SparkSession: spark session with the spark-excel package
    """
    return SparkSession.builder.appName("My Spark ETL Session").config(
        'spark.jars.packages', 'com.crealytics:spark-excel_2.12:3.2.1_0.17.0'
    ).config(
    'spark.hive.metastore.uris', 'thrift://localhost:9083'
    ).enableHiveSupport().getOrCreate()


if __name__ == "__main__":
    spark_session = get_spark_session()
    input_df = get_raw_electricity_data(spark_session, "*")
    data_to_be_saved = data_transformation(input_df)

    table_name = f"full_electricity_{uuid4().hex.upper()[:6]}"

    data_to_be_saved.write.saveAsTable(table_name)

    print(
        spark_session.sql(f"select * from {table_name}").count()
    )
