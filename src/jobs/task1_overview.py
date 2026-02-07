from pyspark.sql import functions as f
from helper.csv_save_helper import save_single_csv
from pyspark.storagelevel import StorageLevel

class OverviewTransformation:
    def __init__(self, spark, df_bronze, output_path):
        self.spark = spark
        self.df_bronze = df_bronze
        self.output_path = output_path
        self.key_conditions = ['Diabetes','Cardiovascular Disease','Asthma', 'Heart disease','Cancer']

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

        filtered = self.df_bronze.filter(
            f.col('Topic').isin(self.key_conditions)
        )

        self.county_aggregated = filtered.groupBy(
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
            .filter(f.col('Topic').isin(self.key_conditions))
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

    def create_dimensions(self):
        
        #Step 2 : Creating Location, Topic, Year and Stratification
        df_dim_location = self.df_bronze.select(
            'LocationID',
            'LocationDesc',
            'LocationAbbr'
        )

        df_dim_location = df_dim_location.dropDuplicates(['LocationID'])

        df_dim_location = df_dim_location.filter(
            f.col('LocationID').isNotNull() &
            f.col('LocationDesc').isNotNull()
        )
        self.df_dim_location_clean = df_dim_location
        df_dim_location.show(10)

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


    def save_to_gold_layer(self):
        print("\n--- Saving Gold Layer Tables ---")

        # Save Fact Table
        save_single_csv(self.fact_chronic_disease, self.output_path, 'fact_chronic_disease')
        save_single_csv(self.df_dim_location_clean, self.output_path, 'dim_location')
        save_single_csv(self.dim_topic, self.output_path, 'dim_topic')
        save_single_csv(self.dim_stratification, self.output_path, 'dim_stratification')
        save_single_csv(self.df_optimized_aggregation, self.output_path, 'fact_county_prevalance')
        save_single_csv(self.dim_year, self.output_path, 'dim_year')

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