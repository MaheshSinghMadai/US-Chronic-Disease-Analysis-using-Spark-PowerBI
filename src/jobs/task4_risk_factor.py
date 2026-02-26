# Task4.py
from pyspark.sql import functions as f
from pyspark.sql import Window
from helper.parquet_save_helper import save_single_parquet

class RiskFactorsTransformation:
    def __init__(self, spark, df_bronze, output_path):
        self.spark = spark
        self.df_bronze = df_bronze
        self.output_path = output_path

        # Customize these to match your dataset content
        self.condition_topics = ["Diabetes", "Cardiovascular Disease", "Heart disease", "Obesity"]
        self.risk_topics = [
            "Nutrition, Physical Activity, and Weight Status",
            "Physical Activity"
        ]
        self.risk_questions_like = ["physical inactivity", "inactive"]  # optional filter

        self.fact_risk_condition_pairs = None
        self.correlation_summary = None
        self.quality_join_report = None

    def run(self):
        print("\n" + "="*80)
        print("TASK 4: TRANSFORMATIONS FOR RISK FACTORS PAGE")
        print("="*80)

        df_risk = self.get_risk_factor_df()
        df_cond = self.get_condition_df()

        self.fact_risk_condition_pairs = self.join_risk_and_conditions(df_risk, df_cond)
        self.correlation_summary = self.compute_correlations(self.fact_risk_condition_pairs)
        self.quality_join_report = self.join_quality_report(df_risk, df_cond, self.fact_risk_condition_pairs)

        self.save_to_gold_layer()

        print("\n" + "="*80)
        print("TASK 4 COMPLETE - Risk factor data ready for Power BI!")
        print("="*80)

        return self.get_results()

    def get_risk_factor_df(self):
        print("\n--- Identifying risk factor records ---")

        df = self.df_bronze.filter(f.col("Topic").isin(self.risk_topics))

        # Optional: narrow to inactivity-related questions if your dataset is wide
        if self.risk_questions_like:
            pattern = "|".join([x.lower() for x in self.risk_questions_like])
            df = df.filter(f.lower(f.col("Question")).rlike(pattern))

        df = (df.select(
                "LocationID", "LocationAbbr", "YearStart",
                f.col("Topic").alias("RiskTopic"),
                f.col("Question").alias("RiskQuestion"),
                f.col("DataValue").alias("RiskValue")
            )
            .filter(f.col("RiskValue").isNotNull())
        )

        df.show(10, truncate=False)
        return df

    def get_condition_df(self):
        print("\n--- Identifying condition records ---")

        df = (self.df_bronze
            .filter(f.col("Topic").isin(self.condition_topics))
            .select(
                "LocationID", "LocationAbbr", "YearStart",
                f.col("Topic").alias("ConditionTopic"),
                f.col("Question").alias("ConditionQuestion"),
                f.col("DataValue").alias("ConditionValue")
            )
            .filter(f.col("ConditionValue").isNotNull())
        )

        df.show(10, truncate=False)
        return df

    def join_risk_and_conditions(self, df_risk, df_cond, min_points_per_state=30):
        print("\n--- Joining risk factors with conditions (LocationID + YearStart) ---")

        # Optimization tip: if df_risk is small vs df_cond, broadcast it:
        # from pyspark.sql.functions import broadcast
        # joined = df_cond.join(broadcast(df_risk), ...)

        joined = (df_cond
            .join(df_risk, on=["LocationID", "LocationAbbr", "YearStart"], how="inner")
            .select(
                "LocationID", "LocationAbbr", "YearStart",
                "RiskTopic", "RiskQuestion", "RiskValue",
                "ConditionTopic", "ConditionQuestion", "ConditionValue"
            )
        )

        # Ensure sufficient data points for correlation (example: per state and pair)
        w = Window.partitionBy("LocationAbbr", "RiskTopic", "ConditionTopic")
        joined = joined.withColumn("StatePairCount", f.count("*").over(w))
        joined = joined.filter(f.col("StatePairCount") >= f.lit(min_points_per_state))

        joined.show(10, truncate=False)
        return joined

    def compute_correlations(self, df_pairs):
        print("\n--- Computing Pearson correlation ---")

        # Global correlations by risk-topic and condition-topic
        corr = (df_pairs
            .groupBy("RiskTopic", "ConditionTopic")
            .agg(
                f.count("*").alias("N"),
                f.round(f.corr("RiskValue", "ConditionValue"), 4).alias("PearsonCorr")
            )
            .orderBy(f.desc("N"))
        )

        corr.show(50, truncate=False)
        return corr

    def join_quality_report(self, df_risk, df_cond, df_joined):
        print("\n--- Join quality check: mismatched joins + null handling strategy ---")

        # Count potential join keys on both sides
        keys_risk = df_risk.select("LocationID", "LocationAbbr", "YearStart").distinct()
        keys_cond = df_cond.select("LocationID", "LocationAbbr", "YearStart").distinct()
        keys_join = df_joined.select("LocationID", "LocationAbbr", "YearStart").distinct()

        report = (keys_cond
            .join(keys_risk, on=["LocationID", "LocationAbbr", "YearStart"], how="left")
            .withColumn("HasRisk", f.when(f.col("LocationID").isNotNull(), 1).otherwise(0))
        )

        # Simpler: overall stats table
        qc = self.spark.createDataFrame([
            ("risk_keys", keys_risk.count()),
            ("condition_keys", keys_cond.count()),
            ("joined_keys", keys_join.count()),
            ("risk_rows", df_risk.count()),
            ("condition_rows", df_cond.count()),
            ("joined_rows", df_joined.count())
        ], ["Metric", "Value"])

        qc.show(truncate=False)

        # Null handling strategy (if you choose left join):
        # - left join conditions to risk, then fill null RiskValue with avg RiskValue per state/year or global avg.
        return qc

    def save_to_gold_layer(self):
        print("\n--- Saving Risk Factors Tables to Gold Layer ---")
        save_single_parquet(self.fact_risk_condition_pairs, self.output_path, "fact_risk_condition_pairs")
        save_single_parquet(self.correlation_summary, self.output_path, "risk_condition_correlation")
        save_single_parquet(self.quality_join_report, self.output_path, "quality_risk_join_report")

    def get_results(self):
        return {
            "fact_risk_condition_pairs": self.fact_risk_condition_pairs,
            "risk_condition_correlation": self.correlation_summary,
            "quality_risk_join_report": self.quality_join_report
        }

def run_risk_factors_transformation(spark, df_bronze, output_path):
    transformer = RiskFactorsTransformation(spark, df_bronze, output_path)
    return transformer.run()