import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from pipelineApplication.Helpers_FunctionsDicts import delete_downloaded_resources, write_bank_json_resources
from pipelineApplication.bronzeLayer.BankData import inst_query, fin_query
from pipelineApplication.bronzeLayer.CreditUnionData import download_cred_zips
from pipelineApplication.bronzeLayer.DataRunParams import DataRunParams, increment_bank_params, currentDate

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

rscDirPath = "./resources"
s3_devpt_url = "s3a://dhill-personal-devpt/alpharank-interview-pipeline"

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


def inst_df_from_resp(inst_json_path: str):
    return spark.read \
        .json(inst_json_path)


def inst_df_to_s3(inst_df: DataFrame):
    inst_df.write.parquet(f"{s3_devpt_url}/bronze/institutions", "append")


financialsSchema = StructType([
    StructField("ASSET", StringType(), nullable=False),
    StructField("CERT", StringType(), nullable=False),
    StructField("DEP", StringType(), nullable=False),
    StructField("ID", StringType(), nullable=False),
    StructField("REPDTE", StringType(), nullable=False)
])


def fin_df_from_resp(fin_json_path: str):
    return spark.read \
        .json(fin_json_path)


def fin_df_to_s3(fin_df: DataFrame):
    fin_df.write.parquet(f"{s3_devpt_url}/bronze/financials", "append")


def csvfile_to_inferred_df(filepath):
    """Caution: Infers schema from CSV file with header."""
    return spark.read \
        .option("inferSchema", True) \
        .option("truncate", False) \
        .csv(filepath, header=True)


def foicu_to_s3(foicu_df: DataFrame):
    foicu_df.write.parquet(f"{s3_devpt_url}/bronze/foicu", "append")


def fs220_to_s3(fs220_df: DataFrame):
    fs220_df.write.parquet(f"{s3_devpt_url}/bronze/fs220", "append")


def fs220d_to_s3(fs220d_df: DataFrame):
    fs220d_df.write.parquet(f"{s3_devpt_url}/bronze/fs220d", "append")


def cred_data_to_s3():
    for dirx in os.scandir(rscDirPath):
        qtr_report_dir = dirx.path
        foicu_df = csvfile_to_inferred_df(f"{qtr_report_dir}/FOICU.txt")
        fs220_df = csvfile_to_inferred_df(f"{qtr_report_dir}/FS220.txt")
        fs220d_df = csvfile_to_inferred_df(f"{qtr_report_dir}/FS220D.txt")
        foicu_to_s3(foicu_df)
        fs220_to_s3(fs220_df)
        fs220d_to_s3(fs220d_df)
    print("Finished uploading credit union data.")


def update_bronze_layer():
    rp = DataRunParams()
    os.mkdir(rscDirPath)
    while rp.certNumStop <= DataRunParams.maxCerts:
        # Read and Write Bank Institutions JSON
        inst_resp = inst_query(rp.certNumStart, rp.certNumStop)
        write_bank_json_resources(inst_resp, rscDirPath, "inst")
        # Read and Write Bank Financials JSON
        fin_resp = fin_query(rp.certNumStart, rp.certNumStop)
        write_bank_json_resources(fin_resp, rscDirPath, "fin")
        # Increment bank data params
        print(f"Writing bank data for charter numbers {rp.certNumStart} through {rp.certNumStop}")
        increment_bank_params(rp)
    # Upload Bank Data to S3
    print("Uploading institutional bank data")
    inst_df = inst_df_from_resp(f"{rscDirPath}/inst.json") \
        .select(f.json_tuple(f.to_json(f.col("data")),
                             "ACTIVE", "CERT", "CITY", "ID", "NAME", "REPDTE", "STNAME", "WEBADDR")) \
        .toDF("ACTIVE", "CERT", "CITY", "ID", "NAME", "REPDTE", "STNAME", "WEBADDR")
    inst_df_to_s3(inst_df)
    print("Uploading financial bank data")
    fin_df = fin_df_from_resp(f"{rscDirPath}/fin.json") \
        .select(f.json_tuple(f.to_json(f.col("data")),
                             "ASSET", "CERT", "DEP", "ID", "REPDTE")) \
        .toDF("ASSET", "CERT", "DEP", "ID", "REPDTE")
    fin_df_to_s3(fin_df)
    print("Finished uploading bank data. Retrieving credit union data...")
    delete_downloaded_resources()
    os.mkdir(rscDirPath)
    # Read and Write Credit Union Data to S3
    download_cred_zips(rscDirPath)  # increments credit union data params
    cred_data_to_s3()
    delete_downloaded_resources()
    # Increment run params
    DataRunParams.prevRun = currentDate
    print(f"Setting previous run date to today's date, and exiting...")


def testing():
    os.mkdir(rscDirPath)
    # Read and Write Bank Institutions
    inst_resp = inst_query()
    write_bank_json_resources(inst_resp, rscDirPath, "inst")
    inst_df = inst_df_from_resp(f"{rscDirPath}/inst.json") \
        .select(f.json_tuple(f.to_json(f.col("data")),
                             "ACTIVE", "CERT", "DEP", "ID", "NAME", "REPDTE", "STNAME", "WEBADDR")) \
        .toDF("ACTIVE", "CERT", "CITY", "ID", "NAME", "REPDTE", "STNAME", "WEBADDR")
    # Read and Write Bank Financials
    fin_resp = fin_query()
    write_bank_json_resources(fin_resp, rscDirPath, "fin")
    fin_df = fin_df_from_resp(f"{rscDirPath}/fin.json") \
        .select(f.json_tuple(f.to_json(f.col("data")),
                             "ASSET", "CERT", "DEP", "ID", "REPDTE")) \
        .toDF("ASSET", "CERT", "DEP", "ID", "REPDTE")
