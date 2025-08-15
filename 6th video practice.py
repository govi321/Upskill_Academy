# Databricks notebook source
# MAGIC %md
# MAGIC ##drop the duplicates sql like
# MAGIC #distinct,
# MAGIC #aggregation.
# MAGIC #row_nummber

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# Initialize SparkSession
spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

# Create a sample DataFrame with duplicate id values
data = [
    (1, "Alice", 1000),
    (2, "Bob", 2000),
    (1, "Alice", 1500),  # Duplicate id
    (3, "Charlie", 3000),
    (2, "Bob", 2500)     # Duplicate id
]

columns = ["id", "name", "salary"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()


# Add a row_number column
df2 = df.withColumn("row", row_number().over(Window.partitionBy("id").orderBy(col("salary").desc())))

print("DataFrame with row numbers:")
df2.show()

# Filter out duplicates, keeping the first occurrence (highest salary)
df_no_duplicates = df2.filter(col("row") == 1).drop("row")

print("DataFrame with duplicates removed:")
df_no_duplicates.show()

# COMMAND ----------

Sample_data = [("Ram", "Technology", 4000),	

    ("Shyam", "Technology", 5600),	

    ("Veer", "Technology", 5100),	

    ("nayva", "Accounts", 4000),	

    ("Ram", "Technology", 4000),	

    ("Vijay", "Accounts", 4300),	

    ("Shivani", "Accounts", 4900),	

    ("dinesh", "Sales", 4000),	

    ("Anupam", "Sales", 3000),	

    ("Anas", "Technology", 5100)	

  ]	


Sample_columns= ["employee_name", "department", "salary"]	


df= spark.createDataFrame(data = Sample_data, schema = Sample_columns)	

df.printSchema()	

df.show(truncate=False)	

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
df2=df.withColumn("row",row_number().over(Window.partitionBy("department").orderBy("salary")))
display(df2)

# COMMAND ----------

display(df2)
df2.filter(df2.row==4).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #seconhighest salary

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Initialize Spark session
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()

# Sample data: Replace this with your actual dataset
data = [
    ("Sales", 50000),
    ("Sales", 45000),
    ("HR", 55000),
    ("HR", 60000),
    ("IT", 70000),
    ("IT", 65000),
    ("IT", 72000),
]

# Create a DataFrame from the sample data
columns = ["department", "salary"]
df = spark.createDataFrame(data, columns)
display(df)



# Add a rank column to the DataFrame based on the window specification
df_with_rank = df.withColumn("rank", rank().over( Window.partitionBy("department").orderBy(df["salary"].desc())
))

# Filter out the rows where rank = 2 to get the 2nd highest salary for each department
second_highest_salaries = df_with_rank.where(df_with_rank["rank"] == 3)

# Show the result
second_highest_salaries.show()

# Stop the Spark session


# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop duplicate by column 

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

# Create a sample DataFrame with duplicate id values
data = [
    (1, "Alice", 1000),
    (2, "Bob", 2000),
    (1, "Alice", 1500),  # Duplicate id
    (3, "Charlie", 3000),
    (2, "Bob", 2500)     # Duplicate id
]

columns = ["id", "name", "salary"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# Remove duplicates based on the 'id' column, keeping the first occurrence
df_no_duplicates = df.dropDuplicates(["id"])

print("unique records:")
df_no_duplicates.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##How to write sql query top f the dataframe
# MAGIC

# COMMAND ----------

empColumns = ["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]

empdata = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (4,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
      ]

empDF = spark.createDataFrame(schema = empColumns, data=empdata)
empDF.show()

# COMMAND ----------

deptdata = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=deptdata, schema = deptColumns)
deptDF.printSchema()
deptDF.show()

# COMMAND ----------

display(empDF)
display(deptDF)

# COMMAND ----------

empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from EMP

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DEPT

# COMMAND ----------

# MAGIC %md
# MAGIC ##self join employee and manager relation

# COMMAND ----------

# MAGIC %sql
# MAGIC select m.name as employeename,e.name as managername from emp as e inner join emp as m on e.emp_dept_id=m.emp_dept_id 

# COMMAND ----------

# MAGIC %md
# MAGIC ##inner join
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp as e inner join dept as d on e.emp_dept_id=d.dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %md
# MAGIC ##departmentwise maximum salary

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_dept_id,max(salary)as maxsal from emp group by emp_dept_id

# COMMAND ----------

# MAGIC %md
# MAGIC # highest salay

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte 
# MAGIC as 
# MAGIC (
# MAGIC   select *,row_number()over (partition by emp_dept_id order by salary desc)as highsal from emp
# MAGIC )
# MAGIC select * from cte where hh=1

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte 
# MAGIC as 
# MAGIC (
# MAGIC   select *,row_number()over (order by salary desc)as highsal from emp
# MAGIC )
# MAGIC select * from cte where highsal=1

# COMMAND ----------

joinDF = spark.sql("select * from EMP as e inner join DEPT as d on d.dept_id=e.emp_dept_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##union (remove the duplicates)

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df1 = spark.createDataFrame(data = simpleData, schema = columns)
display(df1)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

display(df2)

# COMMAND ----------

df3= df1.union(df2).distinct()
display(df3)


# COMMAND ----------

# MAGIC %md
# MAGIC ## collect
# MAGIC ## column to row

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("SampleDataFrame").getOrCreate()

# Sample data
data = [
    ("Alice", 25, "New York"),
    ("Bob", 30, "Los Angeles"),
    ("Charlie", 35, "Chicago"),
    ("David", 40, "Houston"),
    ("Eva", 45, "Phoenix")
]

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

# COMMAND ----------

display(df)
df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Row to column explode

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Create SparkSession
spark = SparkSession.builder \
    .appName("Explode Example") \
    .getOrCreate()

# Sample data
data = [("John", ["apple", "banana", "orange"]),
        ("Doe", ["grape", "melon"])]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "data"])
display(df)

# Explode the data column

# COMMAND ----------

exploded_df = df.select("name", explode("data").alias("fruit"))

exploded_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove the dupliactes

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

# Sample data
simpleData = [
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("James", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000)
]

# Column names
columns = ["employee_name", "department", "state", "salary", "age", "bonus"]

# Create DataFrame
df1 = spark.createDataFrame(data=simpleData, schema=columns)

# Display the original DataFrame
df1.show()

# Select distinct department and employee_name
newdf = df1.select("department", "employee_name").dropDuplicates()

# Display the new DataFrame without duplicates
newdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##fillna is used to find the null, and replace the another vaues

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Fillna Example").getOrCreate()

# Define the data with some null values
data = [
    ('Alice', 25, 'New York', 70000),
    ('Bob', None, None, 80000),
    ('Charlie', 35, 'Chicago', None),
    ('David', None, 'Houston', 90000)
]

# Define the schema (column names)
columns = ['Name', 'Age', 'City', 'Salary']

# Create the DataFrame
df = spark.createDataFrame(data, schema=columns)

print("Original DataFrame:")
df.show()

# Fill null values with specific values
filled_df = df.fillna({'Age': 0, 'City': 'Unknown', 'Salary': 0})

print("with out nullvalues:")
filled_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##case statment when state is present or not

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create a Spark session
spark = SparkSession.builder.appName("StatusTransformation").getOrCreate()

# Define the data and columns
data = [("Deepak", "05-01-2021", "Y"),
        ("Dinesh", "06-01-2021", "N"),
        ("kym", "06-01-2021", "P"),
        ("Ravi", "07-01-2021", "Y")]

columns = ["EmpName", "DateOfJoining", "Status"]

# Create the DataFrame df_emp
df_emp = spark.createDataFrame(data, columns)
display(df_emp)

# COMMAND ----------

# Transform the Status column using when
df = df_emp.withColumn("Status",
    when(col("Status") == "Y", "Active").
    when(col("Status") == "P", "Present")
    .otherwise("InActive"))

# Show the resulting DataFrame
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## In ADB monotonical increasing in sql it is in (identity 1,1) provide the unique value

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Initialize a SparkSession
spark = SparkSession.builder.appName("AutoGenerateSno").getOrCreate()

# Sample data
data = [("John", "New York"),
        ("Alice", "Los Angeles"),
        ("Bob", "Chicago")]

# Create a DataFrame
df = spark.createDataFrame(data, ["name", "city"])
display(df)

# COMMAND ----------

# Add a new column "sno" with auto-generated sequential numbers
df_with_sno = df.withColumn("unique", monotonically_increasing_id())

# Show the resulting DataFrame
df_with_sno.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##provide the unique number to data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import xxhash64

# Create Spark session
spark = SparkSession.builder.appName("HashIDExample").getOrCreate()

# Sample DataFrame
data = [("Alice", 23), ("Bob", 30), ("Cathy", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Generate a unique hash ID using xxhash64 on name and age columns
df_with_hash = df.withColumn("unique_id", xxhash64("name", "age"))

df_with_hash.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > ##supparate name and numbers by using  Regularexpression command

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Create a Spark session
spark = SparkSession.builder.appName("RemoveNumbers").getOrCreate()

# Sample DataFrame with a "names" column
data = [("Yughandar123",),
        ("siva456",),
        ("Alice",),
        ("kush123456",)]
columns = ["names"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------


# Remove numbers from the "names" column using regexp_replace
df_without_numbers = df.withColumn("names_without_numbers", regexp_replace("names", r'\d+', ''))

df_without_numbers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##pivot row to column

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("PivotExample").getOrCreate()

# Create the initial DataFrame
data = [("Banana", 1000, "USA"),
        ("Carrots", 1500, "USA"),
        ("Banana", 400, "China")]
columns = ["Product", "Amount", "Country"]

df = spark.createDataFrame(data, columns)
display(df)

# Pivot the DataFrame
# Pivot the DataFrame
new_df = df.groupBy("Product").pivot("Country").agg(F.sum("Amount"))

# Show the result
new_df.show()


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #convert huge number to formate number

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, col

# Create Spark session
spark = SparkSession.builder.appName("FormatNumberExample").getOrCreate()

# Sample DataFrame with floating numbers
data = [("Alice", 12345.6789), ("Bob", 9876543.21), ("Cathy", 1000.5)]
columns = ["name", "salary"]
df = spark.createDataFrame(data, columns)

# Format salary with commas and 2 decimal places
df_formatted = df.withColumn("formatted_salary", format_number(col("salary"), 2))

df_formatted.show(truncate=False)
