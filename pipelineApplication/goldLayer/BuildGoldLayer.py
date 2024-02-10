"""
Script code for persisting datasets to S3, prepared for analysis in Athena as Glue tables.

Contains:
    s3_devpt_url: str - URL to development S3 storage bucket.\n
    silver_schema: StructType - Schema for DataFrame of cleaned dataset in S3 datalake.\n
    silver_data() - Function reads in dataset from silver layer in S3 as DataFrame.\n
    quarter_dates_df() - Function creates DataFrame listing unique dates within cleaned dataset.\n
    compile_quarterly_assets_table() - Function creates DataFrame that lists each financial institution's total assets data by quarter.\n
    compile_quarterly_deposits_table() - Function creates DataFrame that lists each financial institution's total deposits data by quarter.\n
    TableDataDFs - Class containing methods that create DataFrames ready for analysis and cataloguing in Glue and Athena.\n
    update_gold_layer() - Function persists datasets prepped for cataloguing as Glue tables to be analyzed in Athena.
"""
import os

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, LongType

from pipelineApplication.Helpers_FunctionsDicts import dateToQtrDict, select_sort_dated_cols

aws_key_id = os.environ.get("AWS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_KEY")
tmp_dir = os.environ.get("TMP")

spark = SparkSession.builder \
    .appName("Alpharank Pipeline") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.caseSensitive", True) \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.buffer.dir", tmp_dir) \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,'
            'org.apache.hadoop:hadoop-aws:3.2.0,'
            'com.amazonaws:aws-java-sdk-bundle:1.11.375,'
            'io.delta:delta-core_2.12:1.0.1') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext

logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logger.Level.OFF)
logger.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(logger.Level.ERROR)

s3_devpt_url = "s3a://dhill-personal-devpt/alpharank-interview-pipeline"
silver_schema = StructType([
    StructField("charter_number", IntegerType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("quarter_date", DateType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("website", StringType(), nullable=False),
    StructField("assets_total", LongType(), nullable=False),
    StructField("deposits_total", LongType(), nullable=False),
    StructField("institution_type", StringType(), nullable=False)
])


# Base and Helper DataFrames
def silver_data() -> DataFrame:
    """
    Reads cleaned consolidated dataset persisted in S3 Silver layer as a DataFrame.

    Returns:
         DataFrame
    """
    df = spark.read.schema(silver_schema).parquet(f"{s3_devpt_url}/silver")
    return df


def quarter_dates_df() -> DataFrame:
    """
    Creates DataFrame consisting of one column of distinct dates in the dataset.

    Returns:
         DataFrame
    """
    qd_df = silver_data() \
        .select("quarter_date") \
        .sort("quarter_date", ascending=False) \
        .dropDuplicates()
    return qd_df


# Compiles Table Datasets
def compile_quarterly_assets_table() -> DataFrame:
    """
    Creates DataFrame that lists each financial institution's total assets data by quarter.

    Returns:
         DataFrame
    """
    component_dfs: list[DataFrame] = []
    combo_df = silver_data().select("charter_number", "name", "institution_type")
    for qdr in quarter_dates_df().collect():
        comp_df = silver_data() \
            .select("charter_number", "institution_type", "quarter_date", "assets_total") \
            .dropDuplicates() \
            .filter(F.col("quarter_date") == F.lit(qdr[0])) \
            .withColumnRenamed("assets_total", f"{qdr[0]}") \
            .drop("quarter_date") \
            .sort("charter_number")
        component_dfs.append(comp_df)
    for df in component_dfs:
        combo_df = combo_df.join(df, ["charter_number", "institution_type"], "full") \
            .dropDuplicates() \
            .sort("charter_number")
    return combo_df


def compile_quarterly_deposits_table() -> DataFrame:
    """
    Creates DataFrame that lists each financial institution's total deposits data by quarter.

    Returns:
         DataFrame
    """
    component_dfs: list[DataFrame] = []
    combo_df = silver_data().select("charter_number", "name", "institution_type")
    for qdr in quarter_dates_df().collect():
        comp_df = silver_data() \
            .select("charter_number", "institution_type", "quarter_date", "deposits_total") \
            .dropDuplicates() \
            .filter(F.col("quarter_date") == F.lit(qdr[0])) \
            .withColumnRenamed("deposits_total", f"{qdr[0]}") \
            .drop("quarter_date") \
            .sort("charter_number")
        component_dfs.append(comp_df)
    for df in component_dfs:
        combo_df = combo_df.join(df, ["charter_number", "institution_type"], "full") \
            .dropDuplicates() \
            .sort("charter_number")
    return combo_df


# Final Table Datasets
class TableDataDFs:
    """
    A class containing methods that create and return DataFrames ready for analysis and cataloguing in Glue and Athena.
    """

    @staticmethod
    def institutions_directory_by_type() -> DataFrame:
        """
        Creates DataFrame of current operational institutions, optimized for analysis by type and name.

        Returns:
             DataFrame
        """
        return silver_data() \
            .select("name", "charter_number", "institution_type", "city", "state", "website") \
            .repartition("institution_type", "name") \
            .sortWithinPartitions("name") \
            .distinct()

    # Explore Assets and Deposits by State and Date
    @staticmethod
    def assets_deposits_by_state() -> DataFrame:
        """
       Creates DataFrame of financial data, optimized for analysis by date and state.

       Returns:
            DataFrame
       """
        return silver_data() \
            .select("charter_number", "name", "state", "city", "assets_total", "deposits_total", "quarter_date") \
            .withColumn("year", F.date_format("quarter_date", "yyyy").cast("integer").alias("year")) \
            .withColumn("qtr_date", F.date_format("quarter_date", "MM-dd")) \
            .replace(dateToQtrDict, subset=["qtr_date"]) \
            .withColumn("quarter", F.col("qtr_date").cast("integer").alias("quarter")) \
            .drop("qtr_date", "quarter_date") \
            .sort("assets_total", ascending=False) \
            .repartition("year", "quarter", "state")

    # Explore Assets over Time
    @staticmethod
    def quarterly_assets_table() -> DataFrame:
        """
        Creates DataFrame of current operational institutions listing out assets total by quarter.

        Returns:
            DataFrame
        """
        return select_sort_dated_cols(
        compile_quarterly_assets_table(),
        ["charter_number", "institution_type", "name"],
        True) \
        .dropna()

    # Explore Deposits over Time
    @staticmethod
    def quarterly_deposits_table():
        """
        Creates DataFrame of current operational institutions listing out deposits total by quarter.

        Returns:
            DataFrame
        """
        return select_sort_dated_cols(
        compile_quarterly_deposits_table(),
        ["charter_number", "institution_type", "name"],
        True) \
        .dropna()


def update_gold_layer():
    """
    Persists datasets prepped for cataloguing as Glue tables to be analyzed in Athena.
    """
    print("Writing gold data layer table data ...")
    TableDataDFs.institutions_directory_by_type().write \
        .partitionBy("institution_type", "state") \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{s3_devpt_url}/gold/institution_directory_by_type")
    TableDataDFs.assets_deposits_by_state().write \
        .partitionBy("year", "quarter", "state") \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{s3_devpt_url}/gold/assets_deposits_by_state")
    TableDataDFs.quarterly_assets_table().write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .save(f"{s3_devpt_url}/gold/quarterly_assets_table")
    TableDataDFs.quarterly_deposits_table().write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .save(f"{s3_devpt_url}/gold/quarterly_deposits_table")
    print("Gold layer data ready for tables.")
