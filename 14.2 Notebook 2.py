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

# MAGIC %run "./14thvideopraticeNotebook1"

# COMMAND ----------

# MAGIC %md
# MAGIC ### joins

# COMMAND ----------

df_join=emp_df .join(dept_df, emp_df.dept_id == dept_df.dept_id, how = "inner")
df_join.show()
display(df_join)
