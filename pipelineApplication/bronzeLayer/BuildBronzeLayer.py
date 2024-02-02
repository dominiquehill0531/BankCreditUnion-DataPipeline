"""
Script code for persisting Bronze layer (unstructured) data to the datastore.

Contains:
    rscDirPath: str - Path to manual temp folder.
    s3_devpt_url: str - URL to development s3 storage bucket.
    df_from_json() - Creates DataFrame from JSON resource.
    inst_df_to_s3() - Writes institutional bank data to S3.
    fin_df_to_s3() - Writes financial bank data to S3.
    csvfile_to_inferred_df() - Creates DataFrame from CSV resource.
    foicu_to_s3() - Writes FOICU table credit union data to S3.
    fs220_to_s3() - Writes  FS220 table credit union data to S3.
    fs220d_to_s3() - Writes FS220D table credit union data to S3.
    cred_data_to_s3() - Writes credit union data to S3.
"""
import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from pipelineApplication.Helpers_FunctionsDicts import delete_downloaded_resources, write_bank_json_resources
from pipelineApplication.bronzeLayer.BankData import inst_query, fin_query
from pipelineApplication.bronzeLayer.CreditUnionData import download_cred_zips
from pipelineApplication.bronzeLayer.DataRunParams import DataRunParams

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
"""
Relative path to a manual temporary folder that will be deleted at end of job stage.\n
Accumulates data from HTTP responses.
"""
s3_devpt_url = "s3a://dhill-personal-devpt/alpharank-interview-pipeline"


def df_from_json(json_path: str) -> DataFrame:
    """
    Creates DataFrame created from JSON resource at the given file path.

    Args:
        json_path: String file path to JSON data.

    Returns:
        DataFrame
    """
    return spark.read \
        .json(json_path)


def inst_df_to_s3(inst_df: DataFrame):
    """
    Writes given DataFrame to Amazon S3 bucket directory for bronze level institutional bank data as parquet files.

    Args:
        inst_df: DataFrame of institutional bank data.
    """
    inst_df.write.parquet(f"{s3_devpt_url}/bronze/institutions", "append")


def fin_df_to_s3(fin_df: DataFrame):
    """
    Writes given DataFrame to Amazon S3 bucket directory for bronze level financial bank data as parquet files.

    Args:
        fin_df: DataFrame of financial bank data.
    """
    fin_df.write.parquet(f"{s3_devpt_url}/bronze/financials", "append")


def csvfile_to_inferred_df(filepath: str) -> DataFrame:
    """
    Creates a DataFrame from CSV file at given file path, inferring the schema and a header.

    Args:
        filepath: String path to a CSV file.

    Returns:
        DataFrame
    """
    return spark.read \
        .option("inferSchema", True) \
        .option("truncate", False) \
        .csv(filepath, header=True)


def foicu_to_s3(foicu_df: DataFrame):
    """
    Writes DataFrame of FOICU table in parquet to Amazon S3 bucket directory for bronze level credit union data.

    Args:
        foicu_df: DataFrame of credit union data.
    """
    foicu_df.write.parquet(f"{s3_devpt_url}/bronze/foicu", "append")


def fs220_to_s3(fs220_df: DataFrame):
    """
    Writes DataFrame of FS220 table in parquet to Amazon S3 bucket directory for bronze level credit union data.

    Args:
        fs220_df: DataFrame of credit union data.
    """
    fs220_df.write.parquet(f"{s3_devpt_url}/bronze/fs220", "append")


def fs220d_to_s3(fs220d_df: DataFrame):
    """
    Writes DataFrame of FS220D table in parquet to Amazon S3 bucket directory for bronze level credit union data.

    Args:
        fs220d_df: DataFrame of credit union data.
    """
    fs220d_df.write.parquet(f"{s3_devpt_url}/bronze/fs220d", "append")


def cred_data_to_s3():
    """
    Loops through each downloaded quarterly credit union report in the application's temp folder,
    extracting selected data from various tables and persisting that data to its correlated directory in Amazon S3.
    """
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
    """
    Creates and updates bronze data layer in S3 development bucket while managing the pipeline's
    running parameters and the manual temp folder.
    """
    rp = DataRunParams()
    os.mkdir(rscDirPath)
    while rp.certNumStop <= DataRunParams.maxCerts:
        # Read and Write Bank Institutions JSON
        inst_resp = inst_query(rp.certNumStart, rp.certNumStop)
        write_bank_json_resources(inst_resp, rscDirPath, "inst")
        # Read and Write Bank Financials JSON
        fin_resp = fin_query(rp, rp.certNumStart, rp.certNumStop)
        write_bank_json_resources(fin_resp, rscDirPath, "fin")
        # Increment bank data params
        print(f"Writing bank data for charter numbers {rp.certNumStart} through {rp.certNumStop}")
        rp.increment_bank_params()
    # Upload Bank Data to S3
    print("Uploading institutional bank data")
    inst_df = df_from_json(f"{rscDirPath}/inst.json") \
        .select(f.json_tuple(f.to_json(f.col("data")),
                             "ACTIVE", "CERT", "CITY", "ID", "NAME", "REPDTE", "STNAME", "WEBADDR")) \
        .toDF("ACTIVE", "CERT", "CITY", "ID", "NAME", "REPDTE", "STNAME", "WEBADDR")
    inst_df_to_s3(inst_df)
    print("Uploading financial bank data")
    fin_df = df_from_json(f"{rscDirPath}/fin.json") \
        .select(f.json_tuple(f.to_json(f.col("data")),
                             "ASSET", "CERT", "DEP", "ID", "REPDTE")) \
        .toDF("ASSET", "CERT", "DEP", "ID", "REPDTE")
    fin_df_to_s3(fin_df)
    print("Finished uploading bank data. Retrieving credit union data...")
    delete_downloaded_resources()
    os.mkdir(rscDirPath)
    # Read and Write Credit Union Data to S3
    download_cred_zips(rscDirPath, rp)  # increments credit union data params
    cred_data_to_s3()
    delete_downloaded_resources()
    # TODO: Add current run date to runLog.txt
    with open("runLog.txt", "a") as runLog:
        runLog.write(f"\n{rp.currentRun}")
    print(f"Continuing to Silver Layer...")
