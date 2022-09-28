from datetime import datetime
from acid.data_processing_acid_data import get_input_df, get_happy_flow
from pyspark.sql.functions import desc, col, expr


def get_late_arriving_facts(spark_session):
    df_input = get_input_df(spark_session)
    df_output = get_happy_flow(df_input)
    corrected_event = df_output.\
        filter("reporting_date = '2022-03-25'").\
        orderBy(desc("time")).limit(1).\
        withColumn('produced_kwh', col('produced_kwh')+1)
    
    new_event = corrected_event.\
        withColumn('produced_kwh', col('produced_kwh')+1).\
        withColumn('time', col('time') + expr('interval 10 minutes'))
    
    return corrected_event.unionAll(new_event)


def get_hudi_as_of():
    from datetime import datetime
    from acid.data_processing_acid_hudi import get_hudi_spark_session
    path = 'file:///app/output_data/acid_example/hudi_spark'
    spark_session = get_hudi_spark_session()
    spark_session.read.\
        format('hudi').\
        option("as.of.instant", datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")).\
        load(path).filter("time = '2022-03-25 13:24:26'").\
        select('_hoodie_commit_time','time', 'produced_kwh').show()


def get_delta_as_of():
    from acid.data_processing_acid_delta import get_delta_spark_session
    path = 'output_data/acid_example/delta_spark'

    spark_session = get_delta_spark_session() 
    spark_session.read.\
        format('delta').\
        option("versionAsOf", 0).\
        load(path).filter("time = '2022-03-25 13:24:26'").orderBy(desc('time')).limit(1).show()


def get_iceberg_as_of():
    from acid.data_processing_acid_iceberg import get_iceberg_spark_session
    from datetime import datetime
    spark_session = get_iceberg_spark_session()

    spark_session.sql(
        f'select * from iceberg_acid_example TIMESTAMP AS OF "{datetime.now()}"'
    ).filter("time = '2022-03-25 13:24:26'").show()


def get_default_spark_as_of():
    from acid.data_processing_acid_default_spark import get_default_spark_session
    from datetime import datetime
    path = 'output_data/acid_example/default_spark'
    spark_session = get_default_spark_session()
    
    spark_session.read.parquet(path).createOrReplaceTempView('default_spark_data')

    sql = 'select time, produced_kwh, reporting_date, reporting_month \
        from ( \
            select time, produced_kwh, reporting_date, reporting_month, \
            row_number() over (partition by time order by load_date desc) as rn \
            from  default_spark_data where load_date <= "{at_moment_in_time}") src where rn = 1'

    at_moment_in_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")
    spark_session.sql(sql.format(at_moment_in_time=at_moment_in_time)).filter("time = '2022-03-25 13:24:26'").show()
    