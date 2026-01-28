from pyspark.sql import functions as f
from pyspark.sql import Window
from helper.csv_save_helper import save_single_csv

class trendsTransformation:
    def __init__(self, spark, df_bronze, output_path):
        self.spark = spark
        self.df_bronze = df_bronze
        self.output_path = output_path
        self.key_conditions = ['Diabetes', 'Cardiovascular Disease', 'Asthma', 'Heart disease','Cancer']

        # Store intermediate results
        self.df_trends = None
        self.df_yearly_trends = None
        self.df_yearly_trends_with_yoy = None
        self.df_yearly_trends_flagged = None
        self.df_national_trends_sorted = None
        self.df_state_trends_sorted = None
        self.quality_check = None

    def run(self):

        print("\n" + "="*80)
        print("TASK 2: TRANSFORMATIONS FOR TRENDS OVER TIME PAGE")
        print("="*80)

        # Execute pipeline steps
        self.calculate_yearly_trends_with_yoy()
        self.perform_quality_checks()
        self.calculate_national_trends()
        self.sort_data_chronologically()
        self.save_to_gold_layer()

        print("\n" + "="*80)
        print("TASK 2 COMPLETE - Trends data ready for Power BI!")
        print("="*80)
        
        return self._get_results()

    def calculate_yearly_trends_with_yoy(self):

        print("\n" + "="*80)
        print("STEP 1: TIME-SERIES AGGREGATIONS WITH YoY CHANGE")
        print("="*80)

        self.df_trends = self.df_bronze.filter(
            f.col('Topic').isin(self.key_conditions)
        )

        # Aggregate by Year, Topic, and State
        print("\nAggregating by Year, Topic, and State...")
        self.df_yearly_trends = self.df_trends.groupBy(
            'YearStart',
            'Topic',
            'LocationAbbr'
        ).agg(
            f.round(f.avg('DataValue'), 2).alias('YearlyAvgPrevalence'),
            f.count('DataValue').alias('RecordCount')
        )

        # Calculate Year-over-Year changes
        print("\nCalculating Year-over-Year changes...")
        self.df_yearly_trends_with_yoy = self._add_yoy_changes(
            self.df_yearly_trends,
            partition_cols=['Topic', 'LocationAbbr'],
            order_col='YearStart',
            value_col='YearlyAvgPrevalence'
        )

        print("\n Sample of yearly trends with YoY change:")
        self.df_yearly_trends_with_yoy.orderBy(
            'LocationAbbr', 'Topic', 'YearStart'
        ).show(20)
        

    def perform_quality_checks(self):

        print("\n" + "="*80)
        print("STEP 2: DATA QUALITY CHECKS")
        print("="*80)

        # Register temp view for SQL queries
        self.df_yearly_trends.createOrReplaceTempView("yearly_trends")

        # Check for incomplete years
        expected_state_count = self.df_bronze.select('LocationAbbr').distinct().count()
        print(f"\nExpected state count: {expected_state_count}")
        
        incomplete_years = self._check_incomplete_years(expected_state_count)
        print("\n Years with incomplete data:")
        if incomplete_years.count() > 0:
            incomplete_years.show()
        else:
            print("None - All years have complete data!")
        
        # Comprehensive quality check
        self.quality_check = self._perform_quality_analysis(expected_state_count)
        print("\n Data quality summary by year:")
        self.quality_check.show()
        
        # Add quality flags to trends data
        self.df_yearly_trends_flagged = self.df_yearly_trends_with_yoy.join(
            self.quality_check.select('YearStart', 'Topic', 'QualityStatus'),
            on=['YearStart', 'Topic'],
            how='left'
        )
        
        print("\n Trends data with quality flags (sample):")
        self.df_yearly_trends_flagged.show(10)

    def calculate_national_trends(self):

        print("\n" + "="*80)
        print("STEP 3: NATIONAL TRENDS AGGREGATION")
        print("="*80)

        print("\nAggregating national trends (averaging across all states)...")
        df_national_trends = self.df_trends.groupBy(
            'YearStart',
            'Topic'
        ).agg(
            f.round(f.avg('DataValue'), 2).alias('NationalAvgPrevalence'),
            f.count('DataValue').alias('TotalRecords')
        )
        
        # Calculate YoY changes for national trends
        print("\nCalculating Year-over-Year changes for national data...")
        df_national_trends_with_yoy = self._add_yoy_changes(
            df_national_trends,
            partition_cols=['Topic'],
            order_col='YearStart',
            value_col='NationalAvgPrevalence'
        )
        
        # Sort chronologically
        self.df_national_trends_sorted = df_national_trends_with_yoy.orderBy(
            'Topic', 'YearStart'
        )

        print("\n National trends (sorted chronologically):")
        self.df_national_trends_sorted.show(20)
        
        
    def sort_data_chronologically(self):
        print("\n--- Ensuring Chronological Sorting ---")
        
        # Sort state-level trends
        self.df_state_trends_sorted = self.df_yearly_trends_flagged.orderBy(
            'LocationAbbr', 
            'Topic', 
            'YearStart'
        )


    def save_to_gold_layer(self):
        print("\n--- Saving Trends Data to Gold Layer ---")
        
        # Save state-level trends
        save_single_csv(
            self.df_state_trends_sorted, 
            self.output_path, 
            'fact_state_trends'
        )
        
        # Save national trends
        save_single_csv(
            self.df_national_trends_sorted, 
            self.output_path, 
            'fact_national_trends'
        )
        
        # Save quality report
        save_single_csv(
            self.quality_check, 
            self.output_path, 
            'data_quality_report'
        )

    def _add_yoy_changes(self, df, partition_cols, order_col, value_col):

        window_spec = Window.partitionBy(*partition_cols).orderBy(order_col)
        
        return df.withColumn(
            'PrevYearPrevalence',
            f.lag(value_col, 1).over(window_spec)
        ).withColumn(
            'YoY_Change',
            f.round(f.col(value_col) - f.col('PrevYearPrevalence'), 2)
        ).withColumn(
            'YoY_ChangePercent',
            f.round(
                ((f.col(value_col) - f.col('PrevYearPrevalence')) / f.col('PrevYearPrevalence')) * 100,
                2
            )
        )
    
    def _check_incomplete_years(self, expected_state_count):
        return self.spark.sql(f"""
            SELECT 
                YearStart,
                Topic,
                COUNT(DISTINCT LocationAbbr) as StateCount,
                {expected_state_count} as ExpectedStateCount,
                CASE 
                    WHEN COUNT(DISTINCT LocationAbbr) < {expected_state_count} 
                    THEN 'INCOMPLETE'
                    ELSE 'COMPLETE'
                END as DataStatus
            FROM yearly_trends
            GROUP BY YearStart, Topic
            HAVING COUNT(DISTINCT LocationAbbr) < {expected_state_count}
            ORDER BY YearStart, Topic
        """)
    
    def _perform_quality_analysis(self, expected_state_count, record_threshold=1000):
        return self.spark.sql(f"""
            SELECT 
                YearStart,
                Topic,
                SUM(RecordCount) as TotalRecords,
                COUNT(DISTINCT LocationAbbr) as StateCount,
                CASE 
                    WHEN SUM(RecordCount) < {record_threshold} THEN 'LOW_RECORDS'
                    WHEN COUNT(DISTINCT LocationAbbr) < {expected_state_count} THEN 'MISSING_STATES'
                    ELSE 'VALID'
                END as QualityStatus
            FROM yearly_trends
            GROUP BY YearStart, Topic
            ORDER BY YearStart, Topic
        """)
    
    def _get_results(self):      
        return {
            'fact_state_trends': self.df_state_trends_sorted,
            'fact_national_trends': self.df_national_trends_sorted,
            'quality_report': self.quality_check
        }
    
def run(spark, df_bronze, output_path='data/gold'):
    transformer = trendsTransformation(spark, df_bronze, output_path)
    return transformer.run()