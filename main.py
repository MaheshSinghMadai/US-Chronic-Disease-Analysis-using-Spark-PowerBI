from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import pandas as pd

spark = SparkSession.builder.appName("CDI Session").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



data = spark.read.option("header", True).option("inferSchema", True) \
.csv("file:///C:/Users/MrKillShOtzz/source/repos/US Chronic Disease Analysis using Spark and Power BI/US_Chronic_Disease_Indicators.csv")

data.printSchema()

#handle null and empty columns
total_rows = data.count()
column_quality = []
for col_name in data.columns:
    non_null_count = (
        data.filter(
                (f.col(col_name).isNotNull()) & (f.trim(f.col(col_name)) != "")
            )
            .count()
        )  

column_quality.append((col_name, non_null_count))

df_column_quality = spark.createDataFrame(column_quality, ["column_name","not_null_count"]) \
    .withColumn("non_null_pct", f.col("not_null_count") / f.lit(total_rows)
)

df_column_quality.show(truncate=False)

columns_to_drop = df_column_quality.filter(f.col("not_null_count") == 0).select("column_name").rdd.flatMap(lambda x:x).collect()
data_cleaned = data.drop(columns_to_drop)


data_silver = (
    data_cleaned.select(
        "LocationDesc",
        "LocationID",
        "StateAbbr",
        "Topic",
        "DataValue",
        "YearStart",
        "StratificationCategory"
    )
)

data_silver = data_silver.filter(
    f.col("LocationID").isNotNull()
)
