# Databricks notebook source
# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Optimizing Joins")
    .master("spark://17e348267994:7077")
    .config("spark.cores.max", 16)
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

spark

# COMMAND ----------

# Disable AQE and Broadcast join

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

_dept_schema = "department_id int, department_name string, description string, city string, state string, country string"

dept = spark.read.format("csv").schema(_dept_schema).option("header", True).load("/FileStore/tables/department_data.txt")

# COMMAND ----------

schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"

emp = spark.read.format("csv").schema(schema).option("header", True).load("/FileStore/tables/employee_records.txt")

# COMMAND ----------

from pyspark.sql.functions import broadcast
df_joined = emp.join(broadcast(dept),emp.department_id = dept.department_id,how = "left_outer")

# COMMAND ----------

df_joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------

df_joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Join Big and Big table - SortMerge without Buckets

# COMMAND ----------


sales_schema = "transacted_at string, trx_id string, retailer_id string, description string, amount double, city_id string"

sales = spark.read.format("csv").schema(sales_schema).option("header", True).load("/FileStore/tables/new_sales.csv")

# COMMAND ----------

city_schema = "city_id string, city string, state string, state_abv string, country string"

city = spark.read.format("csv").schema(city_schema).option("header", True).load("/FileStore/tables/cities.csv")

# COMMAND ----------

# Join Data

df_sales_joined = sales.join(city, on=sales.city_id==city.city_id, how="left_outer")

# COMMAND ----------

df_sales_joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# Write Sales data in Buckets

sales.write.format("csv").mode("overwrite").bucketBy(4, "city_id").option("header", True).option("path", "/data/input/datasets/sales_bucket.csv").saveAsTable("sales_bucket")

# COMMAND ----------

# Write City data in Buckets

city.write.format("csv").mode("overwrite").bucketBy(4, "city_id").option("header", True).option("path", "/data/input/datasets/city_bucket.csv").saveAsTable("city_bucket")

# COMMAND ----------

spark.sql("show tables in default").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Join Sales and City data - SortMerge with Bucket

# COMMAND ----------

sales_bucket = spark.read.table("sales_bucket")

# COMMAND ----------

# Read City table

city_bucket = spark.read.table("city_bucket")

# COMMAND ----------

df_joined_bucket = sales_bucket.join(city_bucket, on=sales_bucket.city_id==city_bucket.city_id, how="left_outer")

# COMMAND ----------


df_joined_bucket.write.format("noop").mode("overwrite").save()

# COMMAND ----------

df_joined_bucket.explain()