# Databricks notebook source
# Step 2- Create deptdf in second notebook

# Create the Department DataFrame

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema for Department DataFrame
dept_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True)
])

# Sample department data
dept_data = [
    (101, "HR"),
    (102, "Engineering"),
    (103, "Marketing")
]

# Create Department DataFrame
dept_df = spark.createDataFrame(data=dept_data, schema=dept_schema)

# Display the Department DataFrame
dept_df.show()

# Register as a temporary view for SQL or share with another notebook
#dept_df.createOrReplaceTempView("Department")


# COMMAND ----------

# MAGIC %run "./notebook 1"

# COMMAND ----------

df_join=emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,"inner")
display(df_join)

# COMMAND ----------

jdbcUsername = "goviadmin"
jdbcPassword = "admin123!@#"
jdbcHostname = "databricstosqlconnecting.database.windows.net"    # hostname is servername
jdbcDatabase = "datbricstosqlconetingdatabase"
jdbcPort = 1433

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"
print(jdbcUrl)


# COMMAND ----------

df1 = spark.read.jdbc(url=jdbcUrl, table="Payments")
display(df1)

# COMMAND ----------

df_filter=df1.filter(df1.PaymentMethod == "Credit Card")
display(df_filter)

# COMMAND ----------

df_filter.write.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "df_filter") \
    .option("goviadmin", jdbcUsername) \
    .option("admin123!@#", jdbcPassword) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .save()
