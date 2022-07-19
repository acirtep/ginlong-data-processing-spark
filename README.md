# ginlong-data-processing-spark
Data processing of Solis inverter data with Spark
This repo is created especially for the electricity data downloaded from Ginlong.
If you want to use other kind of data make sure to change the data transformation code, 
in order to accomodate your use case.
This is just a show-case repo for Apache Spark.

1. Create the image `docker build . -t ginlong-data-processing-spark`
2. Access image bash
```
docker run \
--mount type=bind,source=$(pwd)/input_data,target=/app/input_data \
--mount type=bind,source=$(pwd)/output_data,target=/app/output_data \
-it ginlong-data-processing-spark bash
```
3. Execute spark code `python data_processing_spark.py`
