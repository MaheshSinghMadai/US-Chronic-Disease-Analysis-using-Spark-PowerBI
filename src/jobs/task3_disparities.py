# Task3.py
from pyspark.sql import functions as f
from helper.parquet_save_helper import save_single_parquet

class DisparitiesTransformation:
    def __init__(self, spark, df_bronze, output_path):
        self.spark = spark
        self.df_bronze = df_bronze
        self.output_path = output_path

        self.allowed_categories = ["Race/Ethnicity", "Age", "Gender"]

        self.dim_demographic = None
        self.fact_disparities_pivot = None
        self.quality_disparities = None

    def run(self):
        print("\n" + "="*80)
        print("TASK 3: TRANSFORMATIONS FOR DISPARITIES PAGE")
        print("="*80)

        df_filtered = self.filter_demographics()
        self.dim_demographic = self.create_dim_demographic(df_filtered)
        self.fact_disparities_pivot = self.build_pivot_fact(df_filtered)
        self.quality_disparities = self.validate_consistency(df_filtered)

        self.save_to_gold_layer()

        print("\n" + "="*80)
        print("TASK 3 COMPLETE - Disparities data ready for Power BI!")
        print("="*80)

        return self.get_results()

    def filter_demographics(self):
        print("\n--- Filtering for Race/Ethnicity, Age, Gender ---")

        df = (self.df_bronze
            .withColumnRenamed("StratificationCategory1", "StratificationCategory")
            .withColumnRenamed("Stratification1", "Stratification")
            .filter(f.col("StratificationCategory").isin(self.allowed_categories))
            .filter(f.col("DataValue").isNotNull())
        )

        df.show(5, truncate=False)
        return df

    def create_dim_demographic(self, df):
        print("\n--- Creating DimDemographic (standardize values) ---")

        dim = (df.select("StratificationCategory", "Stratification")
            .distinct()
            .withColumn("StratificationCategory", f.initcap(f.trim(f.col("StratificationCategory"))))
            .withColumn("Stratification", f.initcap(f.trim(f.col("Stratification"))))
        )

        dim.show(10, truncate=False)
        return dim

    def build_pivot_fact(self, df):
        print("\n--- Pivoting disparities by stratification (wide format) ---")
        # Pivot within each category separately, then union if you want separate tables.
        # For Power BI stacked bars, long format is often better; but task asked pivot, so we pivot.

        df_std = (df
            .withColumn("StratificationCategory", f.initcap(f.trim("StratificationCategory")))
            .withColumn("Stratification", f.initcap(f.trim("Stratification")))
        )

        # Build pivot key to avoid column-name collisions across categories
        df_std = df_std.withColumn(
            "PivotCol",
            f.concat_ws("_", f.col("StratificationCategory"), f.col("Stratification"))
        )

        fact = (df_std
            .groupBy("Topic", "LocationAbbr", "YearStart")
            .pivot("PivotCol")
            .agg(f.round(f.avg("DataValue"), 2))
        )

        fact.show(10, truncate=False)
        return fact

    def validate_consistency(self, df, tolerance=15.0):
        print("\n--- Quality validation: Overall vs sum of subgroups (approx check) ---")

        # Extract Overall
        overall = (self.df_bronze
            .filter(f.col("StratificationCategory1") == "Overall")
            .select("Topic", "LocationAbbr", "YearStart", f.col("DataValue").alias("OverallValue"))
            .filter(f.col("OverallValue").isNotNull())
        )

        demo = (df
            .groupBy("Topic", "LocationAbbr", "YearStart", "StratificationCategory")
            .agg(f.sum("DataValue").alias("SumSubgroups"))
        )

        qc = (demo
            .join(overall, on=["Topic", "LocationAbbr", "YearStart"], how="left")
            .withColumn("Diff", f.round(f.col("SumSubgroups") - f.col("OverallValue"), 2))
            .withColumn("AbsDiff", f.abs(f.col("Diff")))
            .withColumn("FlagLargeMismatch", f.col("AbsDiff") > f.lit(tolerance))
        )

        qc.filter(f.col("FlagLargeMismatch") == True).show(20, truncate=False)
        return qc

    def save_to_gold_layer(self):
        print("\n--- Saving Disparities Tables to Gold Layer ---")
        save_single_parquet(self.dim_demographic, self.output_path, "dim_demographic")
        save_single_parquet(self.fact_disparities_pivot, self.output_path, "fact_disparities_pivot")
        save_single_parquet(self.quality_disparities, self.output_path, "quality_disparities_report")

    def get_results(self):
        return {
            "dim_demographic": self.dim_demographic,
            "fact_disparities_pivot": self.fact_disparities_pivot,
            "quality_disparities_report": self.quality_disparities
        }

def run_disparities_transformation(spark, df_bronze, output_path):
    transformer = DisparitiesTransformation(spark, df_bronze, output_path)
    return transformer.run()