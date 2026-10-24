-- replace <your_s3_bucket_name> with your s3 bucket name

CREATE DATABASE IF NOT EXISTS prd;

CREATE EXTERNAL TABLE IF NOT EXISTS prd.dim_date
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/dim_date/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.dim_customer
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/dim_customer/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.dim_dealer
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/dim_dealer/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.dim_operation
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/dim_operation/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.dim_part
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/dim_part/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.dim_vehicle
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/dim_vehicle/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.fact_service
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/fact_service/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS prd.job_control
LOCATION 's3://<your_s3_bucket_name>/pyspark-dw-aws/warehouse/prd.db/job_control/'
TBLPROPERTIES ('table_type' = 'DELTA');
