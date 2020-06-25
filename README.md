# Design Document


## 1. Considerations


#### 1.1 Assumptions
There are a few assumptions made about the state of the tables.
1. These tables are initialized with mock data of 5000 rows each.
   ```
   companies
   customers
   products 
   orders
   ```
   And each new day, `2000` rows are generated for below:
   ```
   lead_csv -> feeds into orders
   companies_csv -> feeds into orders
   weblogs
   ```
1. Though mock data are somewhat random, the relationships between tables and fields have been preserved. For more details about the mocking process, see this class `com.comp.utils.Utils` in `spark_etl`

#### 1.2 Constraints
1. An ETL pipeline with daily refresh and reports generation.
1. My laptop has limited resources. Thus, I leveraged AWS for part of the project.

#### 1.3 System Environment
1. Apache Airflow as the job orchestrator
1. AWS Redshift as the B2B backend storage. Was planning to use PostgreSQL but was not cheap to start a small cluster and Redshift is free. PostgreSQL/MySQL should be used in production environment. Redshift does not offer low latency query time
1. AWS S3 as the file storage system
1. Apache Spark as the ETL engine
1. Chartio as the dashboard


'## 2. Architecture


#### 2.1 Overview
*As a B2B platform, the UI will read and write to the backend frequently. To capture all the changes to the db in realtime, the transaction log needs to be streamed to a message queue, like Kinesis/Kafka/MQ. This change log will be used as a master file for a delta lake, which is the difference between the latest db snapshot and the changes since then. The streaming pipeline enables faster analytics and is basically incremental load on steroid. However, creating a streaming pipeline with delta lake is not the scope of this project. I will stick with the batch approach.*

#### 2.2 ETL
*There are 3 dags in Airflow:*
```
initial_data_db_load
daily_etl
daily_incremental_load_companies
```
1. `initial_data_db_load` - initializes the 4 tables mentioned above
1. `daily_etl` - creates new daily csv and web log files, then performs metrics calculations and sends results back to redshift
1. `daily_incremental_load_companies` - mimics the cases where an insert/update happens to a table

### 2.3 S3 File structures
1. Raw init csv location: `s3://com.comp.prod.data.etl/data/init/[companies/orders/products/customers].csv`
1. Raw daily csv and weblog locations: `s3://com.comp.prod.data.etl/data/raw/2020-06-22/[leadcsv.csv/companycsv.csv/weblogs.log]`
1. Final table forms in parquet: `s3://com.comp.prod.data.etl/data/final/table=products/2020-06-22/part....snappy.parquet`
1. Final reports in csv: `s3://com.comp.prod.data.etl/data/final/report=monthly_sales/2020-06-22/part....csv`

