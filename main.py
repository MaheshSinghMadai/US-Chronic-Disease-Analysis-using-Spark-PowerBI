from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from helper.csv_save_helper import save_single_csv

spark = SparkSession.builder.appName("CDI Session") \
    .appName("CDC_Chronic_Disease_Transformations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_bronze = spark.read.option("header", True).option("inferSchema", True) \
.csv("file:///C:/Users/MrKillShOtzz/source/repos/US Chronic Disease Analysis using Spark and Power BI/US_Chronic_Disease_Indicators.csv")

print("Bronze layer loaded successfully")
print(f"Total records: {df_bronze.count()}")
df_bronze.printSchema()


# Transformation for Overview Page

print("\n" + "="*80)
print("TASK 1: TRANSFORMATIONS FOR OVERVIEW PAGE")
print("="*80)

# Step 1: Filter for key conditions and aggregate by county
print("\n--- Step 1: Filtering and Aggregating by County ---")

# Define key conditions to filter
key_conditions = ['Diabetes', 'Cardiovascular Disease', 'Asthma', 'Heart disease','Cancer']

df_filtered = df_bronze.filter(
    f.col('Topic').isin(key_conditions)
)

df_county_aggregated = df_filtered.groupBy(
    'LocationDesc',
    'LocationID',
    'Topic'
).agg(
    f.round(f.avg('DataValue'),2).alias('AvgPrevalance')
)

print("Sample of county aggregated data:")
df_county_aggregated.show(10)


#Step 2 : Creating Location dimension table (DimLocation) 
df_dim_location = df_bronze.select(
    'LocationID',
    'LocationDesc',
    'LocationAbbr'
)

df_dim_location = df_dim_location.dropDuplicates(['LocationID'])

df_dim_location_clean = df_dim_location.filter(
    f.col('LocationID').isNotNull() &
    f.col('LocationDesc').isNotNull()
)
df_dim_location_clean.show(10)


#Step-3 Optimize aggregation for the large datasets using Partitioning and Caching strategy
df_filtered_partitioned = df_bronze.filter(f.col('Topic').isin(key_conditions)) \
    .repartition(50,'LocationAbbr')

df_optimized_aggregation = df_filtered_partitioned.groupBy(
    'LocationDesc',
    'LocationID',
    'Topic',
    'LocationAbbr'
).agg(
    f.round(f.avg('DataValue'),2).alias('AvgPrevalance')
)

print("\nOptimized aggregation with partitioning:")
print(f"Number of partitions: {df_optimized_aggregation.rdd.getNumPartitions()}")

# Additional optimization: Cache if reusing
df_optimized_aggregation.cache()


print("\n" + "="*80)
print("PREPARING GOLD-LAYER TABLES FOR POWER BI")
print("="*80)

# Fact Table: FactChronicDisease
print("\n--- Creating Fact Table ---")

fact_chronic_disease = df_bronze.select(
    f.col('YearStart').alias('Year'),
    f.col('LocationID'),
    f.col('Topic'),
    f.col('Question'),
    f.col('DataValue').alias('Prevalence'),
    f.col('StratificationCategory1').alias('StratificationCategory'),
    f.col('Stratification1').alias('Stratification')
).filter(
    f.col('DataValue').isNotNull()
)

print("Fact table sample:")
fact_chronic_disease.show(10)

# Dimension table: Topic
dim_topic = df_bronze.select(
    'Topic',
    'Question'
).distinct()


print("DimTopic sample: ")
dim_topic.show(10)

# Dimension Table: DimStratification
print("\n--- Creating DimStratification Dimension ---")

dim_stratification = df_bronze.select(
    f.col('StratificationCategory1').alias('StratificationCategory'),
    f.col('Stratification1').alias('Stratification')
).distinct()

print("DimStratification sample:")
dim_stratification.show(10)

print("\n--- Saving Gold Layer Tables ---")

# Save Fact Table
save_single_csv(fact_chronic_disease, 'gold', 'fact_chronic_disease')
save_single_csv(df_dim_location_clean, 'gold', 'dim_location')
save_single_csv(dim_topic, 'gold', 'dim_topic')
save_single_csv(dim_stratification, 'gold', 'dim_stratification')
save_single_csv(df_optimized_aggregation, 'gold', 'fact_county_prevalance')

print("\nAll Gold layer tables saved successfully!")