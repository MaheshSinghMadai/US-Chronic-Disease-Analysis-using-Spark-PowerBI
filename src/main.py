from pyspark.sql import SparkSession
from jobs import task1_overview

spark = SparkSession.builder.appName("CDI Session") \
    .appName("CDC_Chronic_Disease_Transformations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_bronze = spark.read.option("header", True).option("inferSchema", True) \
.csv("file:///C:/Users/MrKillShOtzz/source/repos/US Chronic Disease Analysis using Spark and Power BI/data/raw/US_Chronic_Disease_Indicators.csv")

print("Bronze layer loaded successfully")
print(f"Total records: {df_bronze.count()}")
df_bronze.printSchema()


# Run task1_overview
task1_overview.run(spark, df_bronze, 'data/gold')

spark.stop()