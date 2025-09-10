# NYC Taxi Data Engineering Pipeline

## Project Overview

This repository demonstrates a **production-grade data engineering
pipeline** for NYC Yellow Taxi data using **Apache Airflow, Dataproc
(PySpark), Snowflake, and BigQuery**.\
The solution supports both **batch** and **streaming** processing modes
to handle high-volume real-time data.

------------------------------------------------------------------------

## Architecture

### Batch Pipeline

-   **Airflow DAG** triggers a **Dataproc Serverless Batch Job** every 3
    hours.
-   Job downloads monthly parquet data from NYC taxi dataset, uploads to
    GCS, and processes it using **PySpark**.
-   Data is cleansed, assigned surrogate keys, and written to
    **Snowflake** for historical analytics.

### Streaming Pipeline

-   **Airflow DAG** fetches the latest NYC taxi data in smaller
    row-groups.
-   Each chunk is processed with Pandas, transformed, and loaded to
    **BigQuery** in near real-time.
-   Row numbering is applied for deduplication & primary key generation.

------------------------------------------------------------------------

## Tools & Technologies

-   **Orchestration**: Apache Airflow (Cloud Composer)
-   **Compute**: Dataproc Serverless (PySpark)
-   **Storage**: Google Cloud Storage, BigQuery, Snowflake
-   **Transformation**: PySpark, Pandas
-   **Visualization**: Databricks, Power BI

------------------------------------------------------------------------

## Repository Structure

    ├── batch_pipeline.py             # Airflow DAG for batch pipeline
    ├── stream_pipeline_2.py          # Airflow DAG for streaming pipeline
    ├── dat_proc_batch_file.py        # PySpark job executed on Dataproc
    ├── conf_airflow_year_month_values.txt  # Config values (YEAR, MONTH)
    ├── command run spark.txt         # Example gcloud dataproc batch submit commands
    ├── Main Project.pdf              # Full project documentation
    ├── nyc taxi cloud.pptx           # Architecture and workflow presentation

------------------------------------------------------------------------

## Setup & Installation

### 1️⃣ Prerequisites

-   **GCP Project** with enabled services:
    -   Dataproc
    -   Composer (Airflow)
    -   BigQuery
    -   GCS Bucket
-   **Snowflake** account with target database & schema created.
-   Install the following Python packages locally for testing:

``` bash
pip install apache-airflow google-cloud-storage pandas pyarrow snowflake-connector-python
```

### 2️⃣ Configure Airflow Variables

In Airflow UI: - `year` → starting year (e.g., `2022`) - `month` →
starting month (e.g., `01`) - `year_stream`, `month_stream` → streaming config values

------------------------------------------------------------------------

## Data Modeling

The processed data is modeled in a **star schema** with: - **Fact
Table**: `YELLOW_TRIPS` containing trip-level data - **Dimension
Columns**: - Pickup/Dropoff Date IDs - Payment Types - Location IDs
(PU/DO) - Row Number (as surrogate primary key)

------------------------------------------------------------------------

## Analytics & Visualization

-   **Snowflake** → used for historical reporting (batch mode)
-   **BigQuery** → powers near real-time dashboards (stream mode)
-   **Power BI / Databricks** → used to visualize and analyze data
    trends

------------------------------------------------------------------------

## Screenshots & Slides

Refer to: - **nyc taxi cloud.pptx** for architecture diagrams.
