import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame


def load_survey_df(spark: SparkSession, data_file) -> DataFrame:
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)


def count_by_country(survey_df: DataFrame):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config() -> SparkConf:
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
        print(f"{key}: {val}")
    return spark_conf
