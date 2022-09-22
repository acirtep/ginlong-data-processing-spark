from pyspark.sql import SparkSession
from acid.data_processing_acid_data import get_data


path = 'output_data/acid_example/default_spark'


def get_default_spark_session():
    spark_session = SparkSession.builder.appName('default').config(
        'spark.ui.enabled', 'false').getOrCreate()
    spark_session.sparkContext.setLogLevel('ERROR')
    return spark_session


def load_default_spark(flow_type='happy'):
    spark_session = get_default_spark_session()
    df_input = get_data(spark_session, flow_type)

    df_input.write.\
        partitionBy("reporting_month").\
        mode('overwrite').\
        format('parquet').\
        save(path)


def read_data():
    spark_session = get_default_spark_session()

    number_of_records = spark_session.read.\
        format('parquet').\
        load(path).\
        count()

    print(f"Number of records: {number_of_records}")

    spark_session.read.\
        format('parquet').\
        load(path).\
        filter("reporting_date = '2022-03-01'").\
        show()


if __name__ == "__main__":
    load_default_spark('happy')
    read_data()
    try:
        load_default_spark('error')
    except Exception:
        pass
    read_data()
