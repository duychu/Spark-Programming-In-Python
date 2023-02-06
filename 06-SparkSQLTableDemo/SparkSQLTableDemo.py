from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # way to store data in internal spark database
    # spark itself is a database so we can store data in spark
    # this is spark MANAGED table
    # supported formats are the same: csv, parquet, json, ... 

    if False:
        flightTimeParquetDF.write \
            .mode("overwrite") \
            .saveAsTable("flight_data_tbl")
    else:
        flightTimeParquetDF.write \
            .format("csv") \
            .mode("overwrite") \
            .bucketBy(5, "OP_CARRIER", "ORIGIN") \
            .sortBy("OP_CARRIER", "ORIGIN") \
            .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
    print(spark.catalog.listTables("AIRLINE_DB"))
