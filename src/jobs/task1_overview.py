from pyspark.sql import functions as f
from helper.csv_save_helper import save_single_csv

class OverviewTransformation:
    def __init__(self, spark, df_bronze, output_path):
        self.spark = spark
        self.df_bronze = df_bronze
        self.output_path = output_path
        self.key_conditions = ['Diabetes','Cardiovascular disease','Asthma', 'Heart disease','Cancer']

        #store intermediate results
        self.df_country_aggregated = None
        self.dim_location = None
        self.df_dim_location_clean = None
        self.df_filtered_partitioned = None
        self.df_optimized_aggregation = None
        self.fact_chronic_disease = None
        self.dim_topic = None
        self.dim_stratification = None
    
    def run(self):

        print("\n" + "="*80)
        print("TASK 1: TRANSFORMATIONS FOR OVERVIEW PAGE")
        print("="*80)

        # Execute pipeline steps
        self.aggregate_Data()
        self.optimize_Aggregation()
        self.create_Dimensions()
        self.create_Fact_Table()
        self.save_to_gold_layer()

        print("\n" + "="*80)
        print("TASK 1 Overview Transformation Complete")
        print("="*80)
        
        return self.get_results()


    def aggregate_Data(self):
        
        # Step 1: Filter for key conditions and aggregate by county
        print("\n--- Step 1: Filtering and Aggregating by County ---")

        df_filtered = self.df_bronze.filter(
            f.col('Topic').isin(self.key_conditions)
        )

        self.df_county_aggregated = df_filtered.groupBy(
            'LocationDesc',
            'LocationID',
            'Topic'
        ).agg(
            f.round(f.avg('DataValue'),2).alias('AvgPrevalance')
        )   

        print("Sample of county aggregated data:")
        self.df_county_aggregated.show(10)

        return self.df_country_aggregated


    def create_Dimensions(self):
        #Step 2 : Creating Location dimension table (DimLocation) 
        df_dim_location = self.df_bronze.select(
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

        # Dimension table: Topic
        dim_topic = self.df_bronze.select(
            'Topic',
            'Question'
        ).distinct()


        print("DimTopic sample: ")
        dim_topic.show(10)

        # Dimension Table: DimStratification
        print("\n--- Creating DimStratification Dimension ---")

        dim_stratification = self.df_bronze.select(
            f.col('StratificationCategory1').alias('StratificationCategory'),
            f.col('Stratification1').alias('Stratification')
        ).distinct()

        print("DimStratification sample:")
        dim_stratification.show(10)


    def optimize_Aggregation(self):

        #Step-3 Optimize aggregation for the large datasets using Partitioning and Caching strategy
        df_filtered_partitioned = self.df_bronze.filter(f.col('Topic').isin(self.key_conditions)) \
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


    def create_Fact_Table(self):
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

        print("\nAll Gold layer tables saved successfully!")

    def get_results(self):      
        return {
                'fact_chronic_disease': self.fact_chronic_disease,
                'fact_country_prevalance': self.df_optimized_aggregation,
                'dim_location': self.dim_location,
                'dim_topic': self.dim_topic,
                'dim_stratifcation': self.dim_stratification
        }
    
def run_overview_transformation(spark, df_bronze, output_path):
    transformer = OverviewTransformation(spark, df_bronze, output_path)
    return transformer.run()