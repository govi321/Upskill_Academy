# Databricks notebook source
# MAGIC %md
# MAGIC ##joins

# COMMAND ----------

empcolumns=["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]


empdata=[(1,"smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",3000),
      (3,"Williams",1,"2010","10","M",3000),
      (4,"Jones",2,"2005","10","F",3000),
      (5,"Brown",2,"2010","40","",3000),
      (6,"Brown",2,"2010","50","",3000),
]
df=spark.createDataFrame(data=empdata,schema=empcolumns)

df.show()
display(df)

# COMMAND ----------

deptcolumn=["dept_id","dept_name","dept_state"]


deptdata=[("10","Sales","CA"),
          ("20","Marketing","NY"),
          ("30","Finanace","NY"),
          ("40","IT","NY")
     ]

df1=spark.createDataFrame(data=deptdata,schema=deptcolumn)
df1.show()
display(df1)          

# COMMAND ----------

# MAGIC %md
# MAGIC ##inner join

# COMMAND ----------

display(df)
display(df1)

# COMMAND ----------


df2=df.join(df1,df.emp_dept_id==df1.dept_id,"inner")
display(df2)

# COMMAND ----------

df2=df.join(df1,df.emp_dept_id==df1.dept_id,"inner").select("emp_id","name","gender","dept_name",df1.dept_id)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##semi join it  look like as inner join

# COMMAND ----------

df2=df.join(df1,df.emp_dept_id==df1.dept_id,"semi")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##anti join or left anti join  it means only left side records only

# COMMAND ----------

df2=df.join(df1,df.emp_dept_id==df1.dept_id,"anti")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##left anti

# COMMAND ----------

df2=df.join(df1,df.emp_dept_id==df1.dept_id,"leftanti")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Narrow Transmission and wide Transmission
# MAGIC
# MAGIC like select max(sal)as kk fro emp 
# MAGIC will get single output

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.appName("TransformExample").getOrCreate()

# Sample data
data = [
    ("Alice", "HR", 30),
    ("Bob", "IT", 45),
    ("Charlie", "HR", 28),
    ("David", "IT", 35),
    ("Eva", "Finance", 40)
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "department", "age"])
df.show()

# COMMAND ----------

# Narrow transformation: filter
df_filtered = df.filter(col("age") > 30)
df_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## example of select deptno,max(sal)as jj from emp group by deptno

# COMMAND ----------

# Wide transformation: groupBy (causes shuffle)
df_grouped = df.groupBy("department").avg("age")
df_grouped.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeDataFrame").getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Designation", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("DateOfJoining", StringType(), True),  # You can parse this as DateType if needed
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True),
    StructField("Email", StringType(), True),
])

# Employee data
employee_data = [
    (101, "Alice", "Smith", "HR", "Manager", 80000, "2015-06-23", 35, "New York", "alice.smith@company.com"),
    (102, "Bob", "Brown", "IT", "Developer", 95000, "2018-08-12", 29, "San Francisco", "bob.brown@company.com"),
    (103, "Charlie", "Davis", "Finance", "Analyst", 70000, "2017-09-15", 40, "Chicago", "charlie.davis@company.com"),
    (104, "Diana", "Wilson", "IT", "Lead", 110000, "2020-01-10", 33, "Boston", "diana.wilson@company.com"),
    (105, "Eve", "Moore", "Marketing", "Executive", 65000, "2016-03-30", 28, "Seattle", "eve.moore@company.com"),
    (106, "Frank", "Taylor", "Finance", "Analyst", 72000, "2019-05-14", 31, "Dallas", "frank.taylor@company.com"),
    (107, "Grace", "Anderson", "HR", "Assistant", 50000, "2014-11-01", 45, "Austin", "grace.anderson@company.com"),
    (108, "Hank", "Thomas", "IT", "Engineer", 85000, "2021-04-20", 30, "Denver", "hank.thomas@company.com"),
    (109, "Ivy", "Jackson", "Marketing", "Coordinator", 60000, "2013-07-22", 36, "Los Angeles", "ivy.jackson@company.com"),
    (110, "Jack", "White", "Finance", "Consultant", 75000, "2012-10-05", 41, "Miami", "jack.white@company.com"),
]

# Create DataFrame
df = spark.createDataFrame(employee_data, schema=schema)

# Show DataFrame
df.show()


# COMMAND ----------

# 1. SELECT: Choose specific columns
selected_df = df.select("EmployeeID", "FirstName", "Department", "Salary")
print("Selected Columns:")
selected_df.show()

# 2. FILTER: Employees with a salary greater than 75,000
filtered_df = df.filter(col("Salary") > 75000)
print("Filtered Employees (Salary > 75000):")
filtered_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #wide transmission two type group by and reduce by

# COMMAND ----------

from pyspark.sql.functions import avg, sum, count

# 1. JOIN: Join the employee DataFrame with a sample Department DataFrame
department_data = [
    ("HR", "Human Resources"),
    ("IT", "Information Technology"),
    ("Finance", "Financial Services"),
    ("Marketing", "Marketing and Sales"),
]
df1 = spark.createDataFrame(department_data, ["Department", "DepartmentName"])

# Perform an INNER JOIN on Department column
joined_df = df.join(df1, on="Department", how="inner")
print("Joined DataFrame:")
joined_df.show()

# 2. GROUP BY: Calculate average salary and employee count by Department
grouped_df = df.groupBy("Department").agg(
    avg("Salary").alias("AverageSalary"),
    count("EmployeeID").alias("EmployeeCount")
)
print("Grouped DataFrame (Average Salary and Employee Count by Department):")
grouped_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reduce by
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GroupByVsReduceByKey").getOrCreate()

data = [
    ("HR", 3000),
    ("IT", 4000),
    ("HR", 3500),
    ("Finance", 4500),
    ("IT", 3800)
]

# Create DataFrame
df = spark.createDataFrame(data, ["department", "salary"])
df.show()

# COMMAND ----------

df_grouped = df.groupBy("department").avg("salary")
df_grouped.show()

# COMMAND ----------

# Convert DataFrame to RDD of (key, value) pairs
rdd = df.rdd.map(lambda row: (row["department"], row["salary"]))

# Use reduceByKey
rdd_reduced = rdd.reduceByKey(lambda a, b: a + b)
rdd_reduced.collect()