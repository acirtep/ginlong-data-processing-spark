from pyspark.sql import SparkSession
from acid.data_processing_acid_data import get_data


path = 'file:///app/output_data/acid_example/hudi_spark'


def get_hudi_spark_session():
    spark_session = SparkSession.builder.appName('hudi').config(
        'spark.ui.enabled', 'false').config(
        'spark.jars.packages', 'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0').config(
        'spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config(
        'spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog').config(
        'spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension').getOrCreate()
    spark_session.sparkContext.setLogLevel('ERROR')
    return spark_session


def load_hudi_spark(flow_type='happy'):
    spark_session = get_hudi_spark_session()
    df_input = get_data(spark_session, flow_type)

    hudi_options = {
        'hoodie.table.name': 'acid_hudi_spark',
        'hoodie.datasource.write.recordkey.field': 'time',
        'hoodie.datasource.write.partitionpath.field': 'reporting_month',
        'hoodie.datasource.write.table.name': 'acid_hudi_spark',
        'hoodie.datasource.write.operation': 'insert_overwrite',
        'hoodie.datasource.write.precombine.field': 'time',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2,
        'hoodie.datasource.write.hive_style_partitioning': 'true'
    }

    df_input.write. \
        options(**hudi_options). \
        mode("append"). \
        format("hudi"). \
        save(path)


def read_data():
    spark_session = get_hudi_spark_session()

    number_of_records = spark_session.read.\
        format('hudi').\
        load(path).\
        count()

    print(f"Number of records: {number_of_records}")

    spark_session.read.\
        format('hudi').\
        load(path).\
        filter("reporting_date = '2022-03-01'").\
        show()


if __name__ == "__main__":
    load_hudi_spark('happy')
    read_data()
    try:
        load_hudi_spark('error')
    except Exception:
        pass
    read_data()
