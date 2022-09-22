from pyspark.sql import SparkSession
from acid.data_processing_acid_data import get_data


path = 'output_data/acid_example/iceberg_spark'


def get_iceberg_spark_session():
    spark_session = SparkSession.builder.appName('iceberg').config(
        'spark.ui.enabled', 'false').config(
        'spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.1').config(
        'spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions').config(
        'spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog').config(
        'spark.sql.catalog.spark_catalog.type', 'hive').config(
        'spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog').getOrCreate()
    spark_session.sparkContext.setLogLevel('ERROR')
    return spark_session


def load_iceberg_spark(flow_type='happy'):
    spark_session = get_iceberg_spark_session()
    df_input = get_data(spark_session, flow_type)

    df_input.write.\
        mode("overwrite").\
        format("iceberg"). \
        save('iceberg_acid_example')


def read_data():
    spark_session = get_iceberg_spark_session()

    number_of_records = spark_session.read.\
        format('iceberg').\
        table('iceberg_acid_example').\
        count()

    print(f"Number of records: {number_of_records}")

    spark_session.read.\
        format('iceberg').\
        table('iceberg_acid_example').\
        filter("reporting_date = '2022-03-01'").\
        show()


def create_table():
    spark_session = get_iceberg_spark_session()

    spark_session.sql(
        "CREATE or REPLACE TABLE iceberg_acid_example (  \
            time timestamp,  \
            produced_kwh decimal,  \
            reporting_date date, \
            reporting_month string ) \
        USING iceberg \
        PARTITIONED BY (reporting_month) \
        LOCATION '/app/output_data/acid_example/iceberg_spark'"
    )


if __name__ == "__main__":
    create_table()
    load_iceberg_spark('happy')
    read_data()
    try:
        load_iceberg_spark('error')
    except Exception:
        pass
    read_data()
