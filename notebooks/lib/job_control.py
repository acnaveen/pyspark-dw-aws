from pyspark.sql.functions import max as spark_max, to_date, col

# Get latest insert timestamp for a table
def get_max_insert_ts(spark, schema_name, table_name):
    try:
        df = spark.read.table("prd.job_control")
        
        # return default low if null
        row = df.where(f"schema_name = '{schema_name}' and table_name = '{table_name}'") \
            .agg(spark_max("insert_ts").alias("max_insert_ts")).first()

        return str(row["max_insert_ts"]) if row["max_insert_ts"] is not None else "1900-01-01 00:00:00.000000"

    except Exception as e:
        return None

# Insert a log row into the job control table
def insert_log(spark, schema_name, table_name, now_ts, run_date) -> bool:
    try:
        df = (
            spark.createDataFrame(
                data=[(schema_name, table_name, now_ts, run_date)],
                schema=["schema_name", "table_name", "insert_ts", "run_date"],
            )
            .withColumn("run_date", to_date(col("run_date"), "yyyyMMdd"))
        )
        df.write.format("delta").mode("append").saveAsTable("prd.job_control")
        return True

    except Exception as e:
        return False