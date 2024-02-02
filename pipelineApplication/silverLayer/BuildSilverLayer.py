import os

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType, LongType, StructType, StringType, StructField

from pipelineApplication.Helpers_FunctionsDicts import value_via_dict
from pipelineApplication.silverLayer.StateAbbreviationDict import states

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
stAbbrevMap = states
b = sc.broadcast(stAbbrevMap)
institutionSchema = StructType([
    StructField("ACTIVE", StringType(), nullable=False),
    StructField("CERT", StringType(), nullable=False),
    StructField("CITY", StringType(), nullable=False),
    StructField("ID", StringType(), nullable=False),
    StructField("NAME", StringType(), nullable=False),
    StructField("REPDTE", StringType(), nullable=True),
    StructField("STNAME", StringType(), nullable=False),
    StructField("WEBADDR", StringType(), nullable=True)
])
financialsSchema = StructType([
    StructField("ASSET", StringType(), nullable=False),
    StructField("CERT", StringType(), nullable=False),
    StructField("DEP", StringType(), nullable=False),
    StructField("ID", StringType(), nullable=False),
    StructField("REPDTE", StringType(), nullable=False)
])


# Access data as dataframe for validation, selection, standardization [vss]
# noinspection SpellCheckingInspection
class BronzeDFs:
    @staticmethod
    def bank_inst(): return spark.read.schema(institutionSchema).parquet(f"{s3_devpt_url}/bronze/institutions")

    @staticmethod
    def bank_fin(): return spark.read.schema(financialsSchema).parquet(f"{s3_devpt_url}/bronze/financials")

    @staticmethod
    def cu_foicu(): return spark.read.parquet(f"{s3_devpt_url}/bronze/foicu")

    @staticmethod
    def cu_fs220(): return spark.read.parquet(f"{s3_devpt_url}/bronze/fs220").drop("ACCT_671")

    @staticmethod
    def cu_fs220d(): return spark.read.parquet(f"{s3_devpt_url}/bronze/fs220d")


def vss_bank_inst(bank_inst_df: DataFrame):
    new_df = bank_inst_df \
        .filter("ACTIVE == 1") \
        .withColumn("charter_number", F.col("CERT").cast(IntegerType()).alias("charter_number")) \
        .withColumn("city", F.initcap(F.col("CITY"))) \
        .withColumn("name", F.upper(F.col("NAME"))) \
        .withColumn("quarter_date",
                    F.date_format(
                        F.when(F.col("REPDTE").rlike("\d{1,2}/\d{1,2}/\d{4}"),
                               F.to_date("REPDTE", "M/d/yyyy")),
                        "yyyy-MM-dd"
                    )) \
        .withColumn("state", F.initcap(F.col("STNAME"))) \
        .withColumn("website", F.lower(F.col("WEBADDR"))) \
        .fillna("Not Provided", "website") \
        .replace("", "Not Provided", ["website"]) \
        .drop("ACTIVE", "CERT", "CITY", "NAME", "ID", "REPDTE", "STNAME", "WEBADDR")
    return new_df


def vss_bank_fin(bank_fin_df: DataFrame):
    new_df = bank_fin_df \
        .withColumn("assets_total", F.col("ASSET").cast(LongType()).alias("assets_total")) \
        .withColumn("charter_number", F.col("CERT").cast(IntegerType()).alias("charter_number")) \
        .withColumn("deposits_total", F.col("DEP").cast(LongType()).alias("deposits_total")) \
        .withColumn("quarter_date",
                    F.date_format(
                        F.when(F.col("REPDTE").rlike("\d{8}"),
                               F.to_date("REPDTE", "yyyyMMdd")),
                        "yyyy-MM-dd"
                    )) \
        .drop("ASSET") \
        .drop("CERT") \
        .drop("DEP") \
        .drop("ID") \
        .drop("REPDTE")
    return new_df


def vss_foicu(foicu_df: DataFrame):
    new_df = foicu_df \
        .select("CU_NUMBER", "CU_NAME", "CITY", "STATE", "CYCLE_DATE") \
        .withColumnRenamed("CU_NUMBER", "charter_number") \
        .withColumn("name", F.upper(F.column("CU_NAME"))) \
        .withColumn("city", F.initcap(F.col("CITY"))) \
        .withColumn("state", value_via_dict(b)(F.col("STATE"))) \
        .withColumn("quarter_date",
                    F.date_format(
                        F.when(F.col("CYCLE_DATE").rlike("\d{1,2}/\d{1,2}/\d{4} 0:00:00"),
                               F.to_date("CYCLE_DATE", "M/dd/yyyy H:mm:ss")),
                        "yyyy-MM-dd"
                    )) \
        .drop("CU_NAME", "CITY", "STATE", "CYCLE_DATE")
    return new_df


def vss_fs220(fs220_df: DataFrame):
    new_df = fs220_df \
        .select("CU_NUMBER", "CYCLE_DATE", "ACCT_010", "ACCT_018") \
        .withColumnRenamed("CU_NUMBER", "charter_number") \
        .withColumn("quarter_date",
                    F.date_format(
                        F.when(F.col("CYCLE_DATE").rlike("\d{1,2}/\d{1,2}/\d{4} 0:00:00"),
                               F.to_date("CYCLE_DATE", "M/dd/yyyy H:mm:ss")),
                        "yyyy-MM-dd"
                    )) \
        .withColumnRenamed("ACCT_010", "assets_total") \
        .withColumnRenamed("ACCT_018", "deposits_total") \
        .drop("CYCLE_DATE")
    return new_df


def vss_fs220d(fs220d_df: DataFrame):
    new_df = fs220d_df \
        .select(["CU_NUMBER", "CYCLE_DATE", "Acct_891"]) \
        .withColumnRenamed("CU_NUMBER", "charter_number") \
        .withColumn("quarter_date",
                    F.date_format(
                        F.when(F.col("CYCLE_DATE").rlike("\d{1,2}/\d{1,2}/\d{4} 0:00:00"),
                               F.to_date("CYCLE_DATE", "M/dd/yyyy H:mm:ss")),
                        "yyyy-MM-dd"
                    )) \
        .withColumn("website", F.lower(F.col("Acct_891"))).fillna("Not Provided", "website") \
        .replace("", "Not Provided", ["website"]) \
        .drop("CYCLE_DATE", "Acct_891")
    return new_df


# Join respective dataframes into Bank and Credit Union Dataframes, with Institution Type columns in each
class CombinedDFs:
    @staticmethod
    def bank_data(): return vss_bank_inst(BronzeDFs.bank_inst()) \
        .drop("quarter_date") \
        .join(vss_bank_fin(BronzeDFs.bank_fin()), "charter_number", "left") \
        .withColumn("quarter_date", F.col("quarter_date").cast("date")) \
        .withColumn("institution_type", F.lit("bank")) \
        .dropDuplicates().dropna()

    @staticmethod
    def cred_u_data(): return vss_foicu(BronzeDFs.cu_foicu()) \
        .join(vss_fs220d(BronzeDFs.cu_fs220d()), ["charter_number", "quarter_date"], "left") \
        .join(vss_fs220(BronzeDFs.cu_fs220()), ["charter_number", "quarter_date"], "left") \
        .withColumn("quarter_date", F.col("quarter_date").cast("date")) \
        .withColumn("institution_type", F.lit("credit union")) \
        .dropDuplicates().dropna()


# Join together combined Silver Layer Dataframe;
# noinspection SpellCheckingInspection
def silver_data(bank_df: DataFrame, credu_df: DataFrame):
    new_df = bank_df \
        .unionByName(credu_df) \
        .distinct().sort("state", "city") \
        .repartition(100, "state", "city")
    return new_df


# Upload Silver Layer data to S3
def update_silver_layer():
    """Appends the validated and standardized dataset to the data in the silver layer in Amazon S3"""
    print("Writing updated silver data layer...")
    silver_data(CombinedDFs.bank_data(), CombinedDFs.cred_u_data()) \
        .write.parquet(f"{s3_devpt_url}/silver", "overwrite")
    print("Silver layer ready in S3.")
