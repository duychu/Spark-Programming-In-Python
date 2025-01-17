import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *
import time

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = (
        SparkSession.builder.appName("HelloSpark")
        .master("local[2]")
        .config(conf=conf)
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark ",
        )
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("INFO")
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    conf_out = spark.sparkContext.getConf().getAll()
    for k, v in conf_out:
        logger.info(f"{k}={v}")
    survey_raw_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    # count_df.show()
    logger.info(count_df.collect())
    # input("Enter to exit")
    logger.info("Finished HelloSpark")
    # time.sleep(100)
    spark.stop()
