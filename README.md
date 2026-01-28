# US Chronic Disease Analysis using Spark and Power BI

A comprehensive data analysis project leveraging Apache Spark for ETL and data processing of US chronic disease indicators, with visualization and reporting through Power BI.

## Overview

This project processes raw US Chronic Disease Indicators data using PySpark to create clean, aggregated datasets (gold layer) suitable for analysis and visualization. The pipeline includes data quality assessments, trend analysis over time, and structured dimensions and facts for business intelligence.

### Overview Dashboard
<img width="1145" height="784" alt="image" src="https://github.com/user-attachments/assets/d3e8cb66-90d0-4697-858c-852d9ce17b6f" />


## Project Structure

```
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── data/
│   ├── raw/
│   │   └── US_Chronic_Disease_Indicators.csv    # Raw source data
│   └── gold/
│       ├── data_quality_report.csv              # Data quality metrics
│       ├── dim_location.csv                     # Location dimension table
│       ├── dim_stratification.csv               # Stratification dimension table
│       ├── dim_topic.csv                        # Disease topic dimension table
│       ├── fact_chronic_disease.csv             # Main fact table
│       ├── fact_county_prevalance.csv           # County-level prevalence facts
│       ├── fact_national_trends.csv             # National trend facts
│       └── fact_state_trends.csv                # State-level trend facts
└── src/
    ├── config.py                     # Configuration and path settings
    ├── main.py                       # Main ETL pipeline entry point
    ├── helper/
    │   └── csv_save_helper.py        # Utility functions for saving CSVs
    └── jobs/
        ├── task1_overview.py         # Overview/aggregation analysis
        └── task2_trends_over_time.py # Temporal trend analysis
```

## Data Model

### Dimension Tables
- **dim_location**: Geographic information (counties, states)
- **dim_stratification**: Demographic stratification (age, gender, race, etc.)
- **dim_topic**: Disease categories and health indicators

### Fact Tables
- **fact_chronic_disease**: Core fact table with measurements and indicators
- **fact_county_prevalance**: County-level prevalence data
- **fact_national_trends**: Aggregated national-level trends
- **fact_state_trends**: State-level trend analysis

### Quality Assurance
- **data_quality_report.csv**: Data validation and quality metrics

## Getting Started

### Prerequisites
- Python 3.7+
- Apache Spark
- Dependencies listed in `requirements.txt`

### Installation

1. Clone or download the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure Apache Spark is properly configured in your environment

### Running the Pipeline

Execute the main ETL pipeline:

```
spark-submit src/main.py
```

This will:
1. Load raw US Chronic Disease Indicators data
2. Run overview analysis (Task 1)
3. Run trend analysis over time (Task 2)
4. Output processed data to the gold layer

## Key Components

### Configuration (`src/config.py`)
- Defines file paths for raw and processed data
- Contains Spark configuration settings
- Sets application name and logging preferences

### Main Pipeline (`src/main.py`)
- Initializes Spark session
- Loads raw CSV data
- Orchestrates task execution
- Manages Spark lifecycle

### Task 1: Overview Analysis (`src/jobs/task1_overview.py`)
- Generates summary statistics
- Creates aggregated views of chronic disease data
- Outputs dimension and fact tables

### Task 2: Trends Over Time (`src/jobs/task2_trends_over_time.py`)
- Analyzes temporal patterns in disease indicators
- Identifies national and state-level trends
- Produces time-series fact tables

### Helper Functions (`src/helper/csv_save_helper.py`)
- Utilities for saving Spark DataFrames to CSV format
- Handles output formatting and file management

## Output

All processed data is written to the `data/gold/` directory in CSV format, ready for:
- Power BI visualization and reporting
- Further analysis and exploration
- Data archival and documentation

## Power BI Integration

The gold layer data is designed for direct import into Power BI:
- Use dimension tables for slicing and filtering
- Build measures from fact table data
- Create relationships using key fields
- Design interactive dashboards and reports


