# ETL Motor Project

## 📌 Overview

The **ETL Motor Project** is a comprehensive data pipeline designed to process and transform motor insurance data using Apache Spark and Delta Lake. The pipeline performs the following steps:

1. **Data Extraction**: Loads raw motor insurance data from Delta tables.
2. **Data Transformation**: Cleanses and enriches data, including date normalization and daily premium allocation.
3. **Data Loading**: Writes the transformed data into a Delta table for downstream analytics.

This project leverages PySpark for distributed data processing and Delta Lake for efficient data storage and ACID transactions.

## ⚙️ Technologies Used

- **Apache Spark**: Distributed data processing engine.
- **Delta Lake**: Storage layer providing ACID transactions.
- **PySpark**: Python API for Spark.
- **Delta Tables**: For reading and writing data.
- **Databricks**: Cloud platform for running Spark jobs (optional).

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.x
- Delta Lake 1.0+
- Databricks account (optional)

### Installation

1. Clone the repository:

   ```python
   git clone https://github.com/dovannam115/ETL_Motor_Project.git
   cd ETL_Motor_Project
   ```
2. Install required Python packages:

   ```python
   pip install pyspark delta-spark
   ```
3. Set up a Spark session:

   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder \
    .appName("MotorDataETL") \
    .getOrCreate()
   ```
4. Load raw data:
   
   ```python
   raw_data = spark.read.format("delta").table("raw_motor_data")
   ```
5. Run the transformation and loading scripts as per your requirements.

## 🔄 Data Flow

The ETL pipeline follows this sequence
1. **Extract**: Load raw data from Delta tables.
2. **Transform**:
   - Normalize issue and expiry dates.
   - Generate daily records between issue and expiry dates.
   - Calculate daily premium percentages.
   - Compute cumulate and adjusted premium percentages.
   - Determine uneamed premium reserve(UPR) rates.
3. **Load**: Write the transformed data into a new Delta table.

## 📊 Output Schema
The final dataset includes the following columns:
   - **date_data**: The date when the data was processed.
   - **std_v_date_issue**: The policy issue date.
   - **std_v_date_expiry**: The policy expiry date.
   - **earn_date**: The date for which the earned premium is calculated.
   - **EARNED_RATE**: The earned premium rate for the given date.
   - **UPR_RATE**: The unearned premium reserve rate for the given date.

## 🧩 Example Code Snippet

   ```python
   from pyspark.sql import functions as F
   from pyspark.sql.window import Window

   # Define window specification
   window_spec = Window.partitionBy("P_DATE_ISSUE", "P_DATE_EXPIRY").orderBy("DAY_SPLIT")

   # Calculate cumulative premium percentage
   std_final = std_days.withColumn(
       "CUMULATIVE_PERCENT_PREMIUM",
       F.sum("PERCENT_PREMIUM").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
   )
   ```

## 📈 Use Cases

   - Insurance Analytics: Calculate earned and unearned premium over time.
   - Financial Reporting: Generate reports for regulatory compliance.
   - Data Warehousing: Integrate with data lakes or data warehouses for further analysis.
     
## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📧 Contact
For inquiries or contributions, please contact dovannama5qtk48@gmail.com.
 
