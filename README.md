# ginlong-data-processing-spark
Data processing of Solis inverter data with Spark

This repo is created especially for the electricity data downloaded from Ginlong.
If you want to use other kind of data make sure to change the data transformation code, 
in order to accomodate your use case.
This is just a show-case repo for Apache Spark.

Pre-requisite: docker and docker-compose

1. Create services `docker-compose build`
2. Run services `docker-compose up`
3. Start ipython by: `docker exec -it ginlong-data-processing-spark_pyspark_app_1 ipython`
and checkout the spark scripts.
- Execute spark code with hive `python /app/src/data_processing_spark_hive.py`

Do not forget to clean up your system with docker rm, rmi or even prune!
