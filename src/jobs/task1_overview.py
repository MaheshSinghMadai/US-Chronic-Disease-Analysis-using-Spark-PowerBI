from pyspark.sql import functions as f
from helper.parquet_save_helper import save_single_parquet
from pyspark.storagelevel import StorageLevel
from pyspark.sql.window import Window

class OverviewTransformation:
    def __init__(self, spark, df_bronze, output_path):
        self.spark = spark
        self.df_bronze = df_bronze
        self.output_path = output_path

        # Final Gold outputs only
        self.fact_chronic_disease = None
        self.df_dim_location_clean = None
        self.dim_topic = None
        self.dim_year = None
        self.dim_stratification = None
        self.fact_county_prevalance = None
        self.df_optimized_aggregation = None
    
    def run(self):
        print("\n" + "="*80)
        print("TASK 1: TRANSFORMATIONS FOR OVERVIEW PAGE")
        print("="*80)

        # Execute pipeline steps
        self.aggregate_Data()
        df_optimized = self.optimize_aggregation()
        self.quality_check_missing_by_county()
        self.attach_quality_flags_to_county_fact()
        self.create_dimensions()
        self.create_fact_table()
        self.fact_county_prevalance = df_optimized
        
        self.save_to_gold_layer()

        print("\n" + "=" * 80)
        print("TASK 1 Overview Transformation Complete")
        print("=" * 80)

        return self.get_results()


    def aggregate_Data(self):     
        # Step 1: Filter for key conditions and aggregate by county
        print("\n--- Step 1: Filtering and Aggregating by County ---")

        self.county_aggregated = self.df_bronze.groupBy(
            'LocationDesc',
            'LocationID',
            'Topic'
        ).agg(
            f.round(f.avg('DataValue'),2).alias('AvgPrevalance')
        )   

        print("Sample of county aggregated data:")
        self.county_aggregated.show(10)


    def optimize_aggregation(self):
        print("\n--- Optimizing Aggregation (Partition + Cache) ---")

        df_filtered = (
            self.df_bronze
            .repartition(50, 'LocationAbbr')
        )

        df_optimized_aggregation = (
            df_filtered
            .groupBy(
                'LocationDesc',
                'LocationID',
                'LocationAbbr',
                'Topic'
            )
            .agg(
                f.round(f.avg('DataValue'), 2).alias('AvgPrevalance')
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        self.df_optimized_aggregation = df_optimized_aggregation

        print(f"Partitions: {df_optimized_aggregation.rdd.getNumPartitions()}")
        df_optimized_aggregation.show(10)

        return df_optimized_aggregation
    
    
    def attach_quality_flags_to_county_fact(self):
        print("\n--- Attaching missing-data flags to county prevalence fact ---")

        qc = self.df_quality_county_missing.select("LocationID", "Topic", "MissingPct", "FlagMissingGt20Pct")

        self.df_optimized_aggregation = (self.df_optimized_aggregation
            .join(qc, on=["LocationID", "Topic"], how="left")
        )
        return self.df_optimized_aggregation


    def create_dim_location_clean(self):
        print("\n--- Creating DimLocation (dedupe + null handling) ---")

        base = self.df_bronze.select(
            f.col("LocationID").cast("string").alias("LocationID"),
            f.col("LocationDesc").cast("string").alias("LocationDesc"),
            f.col("LocationAbbr").cast("string").alias("StateAbbr")     
        )

        # Score rows: prefer non-null desc and state, and longer descriptions (often better)
        scored = (base
            .withColumn("HasDesc", f.when(f.col("LocationDesc").isNotNull(), 1).otherwise(0))
            .withColumn("HasState", f.when(f.col("StateAbbr").isNotNull(), 1).otherwise(0))
            .withColumn("DescLen", f.length(f.col("LocationDesc")))
        )

        w = Window.partitionBy("LocationID").orderBy(
            f.desc("HasDesc"), f.desc("HasState"), f.desc("DescLen")
        )

        dim_location = (scored
            .withColumn("rn", f.row_number().over(w))
            .filter(f.col("rn") == 1)
            .drop("rn", "HasDesc", "HasState", "DescLen")
            .filter(f.col("LocationID").isNotNull())   # must have key
        )

        # Optional: if some locations still have null desc, you can fill with LocationID
        dim_location = dim_location.withColumn(
            "LocationDesc",
            f.coalesce(f.col("LocationDesc"), f.col("LocationID"))
        )

        self.df_dim_location_clean = dim_location
        dim_location.show(10, truncate=False)
        return dim_location


    def create_dimensions(self):   
        #Step 2 : Creating Location, Topic, Year and Stratification   
        
        #Location dimension
        self.create_dim_location_clean()

        # Topic Dimension
        dim_topic = self.df_bronze.select(
            'Topic',
            'Question'
        ).distinct()

        self.dim_topic = dim_topic

        print("DimTopic sample: ")
        dim_topic.show(10)

        # Stratification Dimension
        print("\n--- Creating DimStratification Dimension ---")

        dim_stratification = self.df_bronze.select(
            f.col('StratificationCategory1').alias('StratificationCategory'),
            f.col('Stratification1').alias('Stratification')
        ).distinct()

        self.dim_stratification = dim_stratification

        print("DimStratification sample:")
        dim_stratification.show(10)


        # Year Dimension
        print("\n--- Creating Year Dimension ---")

        dim_year = (
        self.df_bronze
            .select(
                f.col("YearStart").alias("YearStart"),
                f.to_date(
                    f.concat_ws("-", f.col("YearStart"), f.lit("01"), f.lit("01"))
                ).alias("Date")
            )
            .distinct()
        )

        dim_year = dim_year.withColumn("Year", f.year(f.col("Date")))

        self.dim_year = dim_year

        print("Year :")
        dim_year.show(10)


    def create_fact_table(self):
        print("\n" + "="*80)
        print("PREPARING GOLD-LAYER TABLES FOR POWER BI")
        print("="*80)

        # Fact Table: FactChronicDisease
        print("\n--- Creating Fact Table ---")

        fact_chronic_disease = self.df_bronze.select(
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

        self.fact_chronic_disease = fact_chronic_disease

        print("Fact table sample:")
        fact_chronic_disease.show(10)

    
    def quality_check_missing_by_county(self):
        print("\n--- Quality Check: Missing DataValue > 20% (by county + topic) ---")

        qc = (self.df_bronze
            .groupBy("LocationID", "Topic")
            .agg(
                f.count("*").alias("TotalRows"),
                f.sum(f.when(f.col("DataValue").isNull(), 1).otherwise(0)).alias("MissingRows")
            )
            .withColumn("MissingPct", f.round((f.col("MissingRows") / f.col("TotalRows")) * 100, 2))
            .withColumn("FlagMissingGt20Pct", f.col("MissingPct") > f.lit(20.0))
        )

        self.df_quality_county_missing = qc

        print("Flagged sample:")
        qc.filter(f.col("FlagMissingGt20Pct") == True).show(20, truncate=False)
        return qc


    def save_to_gold_layer(self):
        print("\n--- Saving Gold Layer Tables ---")

        # Save Fact Table
        save_single_parquet(self.fact_chronic_disease, self.output_path, 'fact_chronic_disease')
        save_single_parquet(self.df_dim_location_clean, self.output_path, 'dim_location')
        save_single_parquet(self.dim_topic, self.output_path, 'dim_topic')
        save_single_parquet(self.dim_stratification, self.output_path, 'dim_stratification')
        save_single_parquet(self.df_optimized_aggregation, self.output_path, 'fact_county_prevalance')
        save_single_parquet(self.dim_year, self.output_path, 'dim_year')

        print("\nAll Gold layer tables saved successfully!")

    def get_results(self):      
        return {
                'fact_chronic_disease': self.fact_chronic_disease,
                'fact_county_prevalance':self.fact_county_prevalance,
                'dim_location': self.df_dim_location_clean,
                'dim_topic': self.dim_topic,
                'dim_stratifcation': self.dim_stratification,
                'dim_year': self.dim_year
        }
    
def run_overview_transformation(spark, df_bronze, output_path):
    transformer = OverviewTransformation(spark, df_bronze, output_path)
    return transformer.run()