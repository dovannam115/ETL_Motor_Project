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

   ```bash
   git clone https://github.com/dovannam115/ETL_Motor_Project.git
   cd ETL_Motor_Project
   ```
2. Install required Python packages:

   ```bash
   pip install pyspark delta-spark
   ```
3. Set up a Spark session:

   ```bash
   from pyspark.sql import SparkSession
   spark = SparkSession.builder \
    .appName("MotorDataETL") \
    .getOrCreate()
   ```
