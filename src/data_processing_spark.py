from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, lit, col, split, regexp_replace, row_number, to_date
)
from pyspark.sql import Window


def get_spark_session():
    """
    Returns:
        SparkSession: spark session with the spark-excel package
    """
    return SparkSession.builder.appName("My Spark ETL Session").config(
        'spark.jars.packages', 'com.crealytics:spark-excel_2.12:3.2.1_0.17.0'
    ).getOrCreate()


def get_raw_electricity_data(spark_session, file_pattern):
    """Retrieve raw electricity data
    dataAddress option defines from which cell the data should be read

    Args:
        spark_session (SparkSession): the spark session with spark-excel package
        file_pattern (str): The pattern for name matching on xlsx files to read

    Returns:
        Spark DataFrame: Spark dataframe containing all the data and 2 extra columns
            input_file_name, which containt the filename where the data resides
            uuid, which is the unique identifier of the record
    """
    input_df = spark_session.read \
        .format("excel")  \
        .option("dataAddress", "A3") \
        .option("header", "true") \
        .option("treatEmptyValuesAsNulls", "false") \
        .option("inferSchema", "true") \
        .load(f"/app/input_data/*{file_pattern}*.xlsx")

    return input_df.withColumn(
        'input_file_name', input_file_name()
    ).withColumn(
        'uuid', lit('uuid()')
    )


def data_transformation(input_df):
    """Transform the raw data into relevant electricity information

    Args:
        input_df (Spark DataFrame): _description_
    """

    data_to_be_saved = input_df.withColumnRenamed(
        'Time', 'time').withColumnRenamed(
        'Daily Generation (Active)(kWh)', 'produced_kwh')[
            ['time','produced_kwh', 'input_file_name']
        ]

    data_to_be_saved = data_to_be_saved.withColumn(
        'download_unixtime', split(
            regexp_replace(
                data_to_be_saved.input_file_name, '.xlsx', ''
            ), 
            '-')[4]
        )


    data_to_be_saved = data_to_be_saved.withColumn(
        'reporting_date', to_date(col('time'),'yyyy-MM-dd')
    ).select('time', 'produced_kwh', 'download_unixtime', 'reporting_date')

    time_window = Window.orderBy(
        'time').partitionBy(
        'reporting_date').orderBy(
            col('time').desc(), 
            col('download_unixtime').desc()
        )

    data_to_be_saved = data_to_be_saved.withColumn(
        'time_row_number', row_number().over(time_window)
    )

    data_to_be_saved = data_to_be_saved.where(col('time_row_number') == 1)

    data_to_be_saved = data_to_be_saved.select('reporting_date', 'time', 'produced_kwh')

    return data_to_be_saved


if __name__ == "__main__":
    spark_session = get_spark_session()
    input_df = get_raw_electricity_data(spark_session, "*")
    data_to_be_saved = data_transformation(input_df)

    data_to_be_saved.write.mode('overwrite').partitionBy('reporting_date').parquet(
        '/app/output_data/default_spark/full_electricity/'
    )

    print(
        spark_session.read.parquet(
        '/app/output_data/default_spark/full_electricity'
        ).count()
    )
