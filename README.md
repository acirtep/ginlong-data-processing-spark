# ginlong-data-processing-spark
Data processing of Solis inverter data with Spark

This repo is created especially for the electricity data downloaded from Ginlong.
If you want to use other kind of data make sure to change the data transformation code, 
in order to accomodate your use case.
This is just a show-case repo for Apache Spark and the code used for the articles written at https://ownyourdata.ai/wp/tag/spark/.

Pre-requisite: docker and docker-compose

1. Create services `docker-compose build`
2. Run services `docker-compose up`
3. Start ipython by: `docker exec -it pyspark_hive ipython`
and checkout the spark scripts.
- Execute spark code with hive `python /app/src/data_processing_spark_hive.py`
- Execute spark code for schema changes `python /app/src/data_processing_spark_schema.py`


# For ACID tests:
- Apache Hudi: `python src/acid/data_processing_acid_hudi.py `
- Apache Iceberg: `python src/acid/data_processing_acid_iceberg.py`
- Delta Lake: ` python src/acid/data_processing_acid_delta.py `
- Default Spark: ` python src/acid/data_processing_acid_default_spark.py `

# For time traveling
Make sure to load data and use the methods from data_processing_time_traveling.py


Do not forget to clean up your system with docker rm, rmi or even prune!
