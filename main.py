from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("CDI Session").getOrCreate()

data = spark.read.option("header", True).option("inferSchema", True).option("multiLine", True).option("escape", "\"").csv("/path/to/U.S._Chronic_Disease_Indicators.csv")

data.printSchema()
data.select("name").show()