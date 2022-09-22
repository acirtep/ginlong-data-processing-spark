from pyspark.sql import SparkSession
from acid.data_processing_acid_data import get_data


path = 'output_data/acid_example/delta_spark'


def get_delta_spark_session():
    spark_session = SparkSession.builder.appName('delta').config(
        'spark.ui.enabled', 'false').config(
        'spark.jars.packages', 'io.delta:delta-core_2.12:2.1.0').config(
        'spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config(
        'spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate()
    spark_session.sparkContext.setLogLevel('ERROR')
    return spark_session


def load_delta_spark(flow_type='happy'):
    spark_session = get_delta_spark_session()
    df_input = get_data(spark_session, flow_type)

    df_input.write.\
        partitionBy("reporting_month").\
        mode("overwrite").\
        format("delta"). \
        save(path)


def read_data():
    spark_session = get_delta_spark_session()

    number_of_records = spark_session.read.\
        format('delta').\
        load(path).\
        count()

    print(f"Number of records: {number_of_records}")

    spark_session.read.\
        format('delta').\
        load(path).\
        filter("reporting_date = '2022-03-01'").\
        show()


if __name__ == "__main__":
    load_delta_spark('happy')
    read_data()
    try:
        load_delta_spark('error')
    except Exception:
        pass
    read_data()
