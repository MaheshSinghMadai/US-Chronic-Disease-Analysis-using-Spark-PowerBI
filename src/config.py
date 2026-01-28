import os

# Get project root (parent of src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define all paths
PATHS = {
    'raw_data': os.path.join(PROJECT_ROOT, 'data', 'raw', 'US_Chronic_Disease_Indicators.csv'),
    'gold_output': os.path.join(PROJECT_ROOT, 'data', 'gold'),
}

# Spark configurations
SPARK_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true'
}

APP_NAME = "CDC_Chronic_Disease_Analysis"