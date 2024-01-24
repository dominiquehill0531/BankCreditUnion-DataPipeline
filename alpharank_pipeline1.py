import os

from pyspark.sql.session import SparkSession

from pipelineApplication.bronzeLayer import BuildBronzeLayer
from pipelineApplication.goldLayer import BuildGoldLayer
from pipelineApplication.silverLayer import BuildSilverLayer

aws_key_id = os.environ.get("AWS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_KEY")
tmp_dir = os.environ.get("TMP")
cwd = os.curdir

spark = SparkSession.builder \
    .appName("Alpharank Pipeline") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.caseSensitive", True) \
    .config("spark.eventLog.enabled", True) \
    .config("spark.eventLog.dir", f"{cwd}/logs") \
    .config("spark.history.fs.logDirectory", f"{cwd}/logs") \
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


def main():
    BuildBronzeLayer.update_bronze_layer()
    BuildSilverLayer.update_silver_layer()
    BuildGoldLayer.update_gold_layer()
    spark.stop()


if __name__ == "__main__":
    main()
