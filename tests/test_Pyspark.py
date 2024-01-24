from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

sparkSession = SparkSession.builder.appName("test").getOrCreate()

logger = sparkSession.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logger.Level.OFF)
logger.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(logger.Level.ERROR)

df: DataFrame = sparkSession.createDataFrame([(1, "value1"), (2, "value2")], ["id", "value"])

df.show()

def test_sparksession_builds():
    assert df.count() == 2
