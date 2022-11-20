import os
from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.acid.data_processing_acid_data import get_data


def get_nessie_spark_session():
    import pynessie
    conf = SparkConf()
    # we need iceberg libraries and the nessie sql extensions
    conf.set(
        "spark.jars.packages",
        f"org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.1,org.projectnessie:nessie-spark-extensions-3.3_2.12:0.44.0",
    )
    # ensure python <-> java interactions are w/ pyarrow
    conf.set("spark.sql.execution.pyarrow.enabled", "true")
    # create catalog dev_catalog as an iceberg catalog
    conf.set("spark.sql.catalog.dev_catalog", "org.apache.iceberg.spark.SparkCatalog")
    # tell the dev_catalog that its a Nessie catalog
    conf.set("spark.sql.catalog.dev_catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    # set the location for Nessie catalog to store data. Spark writes to this directory
    conf.set("spark.sql.catalog.dev_catalog.warehouse", "file:///app/output_data/nessie")
    # set the location of the nessie server. In this demo its running locally. There are many ways to run it (see https://projectnessie.org/try/)
    conf.set("spark.sql.catalog.dev_catalog.uri", "http://nessie:19120/api/v1")
    # default branch for Nessie catalog to work on
    conf.set("spark.sql.catalog.dev_catalog.ref", "main")
    # use no authorization. Options are NONE AWS BASIC and aws implies running Nessie on a lambda
    conf.set("spark.sql.catalog.dev_catalog.auth_type", "NONE")
    # enable the extensions for both Nessie and Iceberg
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    # finally, start up the Spark server
    return SparkSession.builder.config(conf=conf).getOrCreate()


def create_table(branch, spark_session):
    spark_session.sql(f"USE REFERENCE {branch} IN dev_catalog")

    spark_session.sql(
        "CREATE or REPLACE TABLE dev_catalog.iceberg_acid_example (  \
            time timestamp,  \
            produced_kwh decimal(18,2),  \
            reporting_date date, \
            reporting_month string ) \
        USING iceberg \
        PARTITIONED BY (reporting_month)"
    )


def load_data(branch, spark_session):
    spark_session.sql(f"USE REFERENCE {branch} IN dev_catalog")
    df_input = get_data(spark_session, "happy")

    df_input.write.\
        mode("overwrite").\
        format("iceberg"). \
        save('dev_catalog.iceberg_acid_example')


def main():
    spark_session = get_nessie_spark_session()
    create_table('main', spark_session)
    load_data('main', spark_session)
