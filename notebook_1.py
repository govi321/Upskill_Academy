# Databricks notebook source
# Step1- create empdf in first notebook

# Create the Employee DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema for Employee DataFrame
emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("dept_id", IntegerType(), True)
])

# Sample employee data
emp_data = [
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 101),
    (4, "David", 103)
]

# Create Employee DataFrame
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

# Display the Employee DataFrame
emp_df.show()

# Register as a temporary view for SQL or share with another notebook
#emp_df.createOrReplaceTempView("Employee")
