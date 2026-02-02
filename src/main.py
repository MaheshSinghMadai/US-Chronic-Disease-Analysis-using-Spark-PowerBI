from pyspark.sql import SparkSession
from jobs import task1_overview, task2_trends_over_time
from config import PATHS, SPARK_CONFIG, APP_NAME

def main():
    builder = SparkSession.builder.appName(APP_NAME)
    
    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(PATHS['raw_data'])
    df_bronze = spark.read.csv(
        f"file:///{PATHS['raw_data']}", 
        header=True, 
        inferSchema=True
    )

    # Run task1_overview
    task1_overview.run_overview_transformation(spark, df_bronze, output_path=PATHS['gold_output'])

    # Run task2_trends_over_time    
    task2_trends_over_time.run(spark, df_bronze, output_path=PATHS['gold_output'])

    spark.stop()

if __name__ == "__main__":
    main()