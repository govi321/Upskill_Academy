# Databricks notebook source
spark.sql("use sai")

# COMMAND ----------

spark.sql("create database saji")

# COMMAND ----------

spark.sql("use saji")

# COMMAND ----------

spark.sql("create table saji.vs(gid int,gname varchar(90),gsal int)")

# COMMAND ----------

spark.sql("insert into saji.vs values(10,'santhosh',333),(20,'meghana',555),(30,'teja',777)")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table saji.vs

# COMMAND ----------

spark.sql ("select * from saji.vs").show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

spark.sql("create database sai")

# COMMAND ----------

spark.sql("use sai")

# COMMAND ----------

spark.sql("create table sai.km(gid int,gname varchar(90))")

# COMMAND ----------

spark.sql("insert into sai.km values(10,'azure'),(20,'eee'),(30,'iti')")

# COMMAND ----------

spark.sql("select * from sai.km").show()

# COMMAND ----------

spark.sql ("select * from saji.vs").show()
spark.sql("select * from sai.km").show()
#inner join between  two tables
spark.sql("select * from saji.vs as v inner join sai.km as k on v.gid=k.gid").show()

# COMMAND ----------

spark.sql("select * from saji.vs as v inner join sai.km as k on v.gid=k.gid").select ("v.gname","v.gsal","k.gname").show()

# COMMAND ----------

#convert table to dataframe
spark.sql("select * from sai.km").show()
df=spark.table("sai.km").collect()
display(df)

# COMMAND ----------

#convert table into dataframe
spark.sql("select * from saji.vs").show()
df1=spark.table("saji.vs").collect()
display()

# COMMAND ----------

from pyspark.sql import SparkSession



# Define schema and data
y = ["eno", "ename", "esal"]
x = [
    (1, "sajida", 500),
    (2, "navya", 700),
    (3, "vs", 500),
    (4, "manish", 1000)
]

# Create DataFrame
df = spark.createDataFrame(data=x, schema=y)

# Display DataFrame
df.show()
display(df)


# COMMAND ----------

#Creatdatframe
df = spark.createDataFrame(data=x, schema=y)

y =["eno","ename","esal"]

x=[
    (1,"sajida",500),
   (2,"navya",700),
   (3,"vs",500),
   (4,"manish",1000)

]
#display
display(df)
df.show()



# COMMAND ----------



# COMMAND ----------

empColumns=["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]
data=[
    (1,"smitha",-1,"2008","10","male",3000),\
    (2,"rose",1,"2010","20","male",4000),\
    (3,"williams",1,"2010","10","male",1000),\
    (4,"jones",-2,"2005","10","female",2000),\
    (5,"brown",2,"2010","40","female",7000),\
    (6,"green",2,"2010","50","male",4000), 
]
empdf=spark.createDataFrame(data=data, schema= empColumns)

#display
display(empdf)
empdf.show()
empdf.printSchema()  


# COMMAND ----------

# MAGIC %md
# MAGIC ##update salary

# COMMAND ----------

df=empdf.withColumn("updatesalary",empdf.salary+1500)
#display
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##renamed updatsalary as amount

# COMMAND ----------

df1=df.withColumnRenamed("updatesalary","Amount")
#display
display(df1)


# COMMAND ----------

empdf.show()
display(empdf)
empdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##joins in dataframe
# MAGIC

# COMMAND ----------

empColumns = ["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]

empdata = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","30","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (4,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
      ]

df = spark.createDataFrame(schema = empColumns, data=empdata)
display(df)

# COMMAND ----------

deptdata = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
df1 = spark.createDataFrame(data=deptdata, schema = deptColumns)
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #joins

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"inner")

display(df2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## i want only some of the columns

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"inner").select("name","emp_id","salary","gender",df1.dept_id)
#display
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##left join

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"left")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##left join with perticular columns

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"left").select("emp_id","name","salary","gender",df.emp_dept_id)
display(df2)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##right join

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"right")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##right join with perticular column

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"right").select("emp_id","name","salary","gender",df1.dept_id,df.emp_dept_id)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##semi **join**
# MAGIC it look like as inner join

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"semi")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #left anti or anti join

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"anti")
display(df2)

# COMMAND ----------

df2 =df.join(df1,df.emp_dept_id==df1.dept_id,"leftanti")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Narrow transformation 
# MAGIC it operates in single partion,examples flatmap(),map(),union(),filter()
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#create spark session
spark= SparkSession.builder.appName("example").getOrCreate()

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

#Narrow Transformation or filter like select * from emp where eno=7

df_filter=df.filter(col("age") >=30)
df_filter.show()                   


# COMMAND ----------

#Narrow Transformation or filter like select * from emp where eno=7

df_filter=df.filter(col("department")=="IT")
df_filter.show()

# COMMAND ----------

#Narrow Transformation or filter like select * from emp where eno=7

df_filter=df.filter(col("name")=="Alice")
df_filter.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##wide transformation

# COMMAND ----------

# MAGIC %md
# MAGIC *while wide transformations require data from multiple partitions, often involving shuffling
# MAGIC Examples: groupByKey(), reduceByKey(), join(), sortByKey()*

# COMMAND ----------

# Wide transformation: groupBy (causes shuffle)

df_grouped=df.groupBy("department").avg("age")
df_grouped.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

# MAGIC %md
# MAGIC  **from the table how many of salary is greter than 75000 sal* as wel as from this in need only EmployeeId,firstName,Department,salary**

# COMMAND ----------

# 1. SELECT: Choose specific columns
select_df=df.select ("EmployeeID","FirstName","Department","Salary")
print("Selected Columns:")
select_df.show()

# 2. FILTER: Employees with a salary greater than 75,000
filter_df=df.filter(col("salary") > 75000)
print("Filtered Employees (Salary > 75000):")
filter_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC *join two tables wide transformation with aggregation*

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import count,sum,max,min,avg,col
#create spark session
spark= SparkSession.builder.appName("example").getOrCreate()

# 1. JOIN: Join the employee DataFrame with a sample Department DataFrame
department_data = [
    ("HR", "Human Resources"),
    ("IT", "Information Technology"),
    ("Finance", "Financial Services"),
    ("Marketing", "Marketing and Sales"),
]

df1 = spark.createDataFrame(department_data, ["Department", "DepartmentName"])

df1.show()
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC * join df and df1

# COMMAND ----------

join_df=df.join(df1, on="Department", how="inner")
print("Joined DataFrame:")
join_df.show()
display(join_df)

# COMMAND ----------

#calculate the avg salary and count wise depatment wise
df_grouped=join_df.groupBy("Department").agg(avg("Salary").alias("avg_salary"),count("EmployeeID").alias("count"))
display(df_grouped)


# COMMAND ----------

# MAGIC %md
# MAGIC ######Diff b/w group by key and reduced by key

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

df_grouped=df.groupBy("department").avg("salary")
df_grouped.show()

# COMMAND ----------

df_grouped=df.groupBy("department").sum("salary")
df_grouped.show()

# COMMAND ----------

df_grouped=df.groupBy("department").max("salary")
df_grouped.show()


# COMMAND ----------

df_grouped=df.groupBy("department").min("salary")
df_grouped.show()

# COMMAND ----------

df_grouped=df.groupBy("department").avg("salary")
df_grouped.show()

# COMMAND ----------

# Convert DataFrame to RDD of (key, value) pairs
rdd = df.rdd.map(lambda row: (row["department"], row["salary"]))

# Use reduceByKey
rdd_reduced = rdd.reduceByKey(lambda a, b: a + b)
rdd_reduced.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC *Remove the duplication in ADB*

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

df = spark.createDataFrame(data,columns)
display(df)

# COMMAND ----------

# Add a row_number column
df2 =df.withColumn("row",row_number().over(Window.partitionBy("id").orderBy(col("id").asc())))
print("DataFrame with row numbers:")
df2.show()

# COMMAND ----------

#remove dulicates
#no_dupliacte_df=df2.filter(col("row")==1).drop("row")
#display(no_dupliacte_df)
# we can use any one of this
df_no_duplicates = df.dropDuplicates(["id"])

print("unique records:")
df_no_duplicates.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###To find  department wise salary###

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
df2 =df.withColumn("kush",row_number().over(Window.partitionBy("department").orderBy(col("salary").desc())))
display(df2)                                           

# COMMAND ----------

# MAGIC %md
# MAGIC ######Department wise second highest sal#####

# COMMAND ----------

df3 = df2.filter(df2.kush==2).show()
display(df3)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Find the second highest salary by using rank in adb####

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


# COMMAND ----------

# Add a rank column to the DataFrame based on the window specification
df_with_rank = df.withColumn("rank", rank().over( Window.partitionBy("department").orderBy(df["salary"].desc())
))
df_with_rank.show()

# COMMAND ----------

# Filter out the rows where rank = 2 to get the 2nd highest salary for each department
second_highest_salaries = df_with_rank.where(df_with_rank["rank"] == 1)

# Show the result
second_highest_salaries.show()

# Stop the Spark session

# COMMAND ----------

# MAGIC %md
# MAGIC ###CrateOrReplaceTempView just like VIEW TABLE IN SQL### 

# COMMAND ----------

# MAGIC %md
# MAGIC *Creating empdf dataframe*

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

# MAGIC %md
# MAGIC *Creating Deptdataframe*

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

#display both dataframes
display(empDF)
display(deptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC create a view table in adb

# COMMAND ----------

empDF.createOrReplaceTempView("emp")
deptDF.createOrReplaceTempView("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dept

# COMMAND ----------

# MAGIC %md
# MAGIC self join by using emp table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select e.name as employee,em.name as managername from emp as e inner join emp as em on e.emp_id=em.superior_emp_id
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC *innner join by using two tables*

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from EMP as e inner join DEPT as d on d.dept_id=emp_dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %md
# MAGIC ###maxsal###

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(salary)as maxsal from emp 

# COMMAND ----------

# MAGIC %md
# MAGIC ##department wise max salary##

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_dept_id,max(salary)as maxsal from emp group by emp_dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte
# MAGIC as
# MAGIC (
# MAGIC   select *,row_number()over(partition by emp_dept_id order by salary desc)as gg from emp
# MAGIC )
# MAGIC select * from cte where gg =1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overall highest sal###

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte
# MAGIC as
# MAGIC (
# MAGIC    select *,row_number()over(order by salary desc)as overallhighsal from emp 
# MAGIC )
# MAGIC select * from cte where overallhighsal=1

# COMMAND ----------

# MAGIC %md
# MAGIC ####inner join####

# COMMAND ----------

joinDF = spark.sql("select * from EMP as e inner join DEPT as d on d.dept_id=e.emp_dept_id").show()
display(joinDF)

# COMMAND ----------

# MAGIC %md
# MAGIC union

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

removedf = df1.union(df2).distinct()
display(removedf)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Row to column##

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
# MAGIC ###column to row###

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

explode_df = df.select("name",explode("data").alias("fruits"))
display(explode_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove dupliaces###

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
# MAGIC ##fillna just like a isnull in sql##

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

# MAGIC %md
# MAGIC ###monotonically increasing like identity(1,1) in sql ###

# COMMAND ----------

# Add a new column "sno" with auto-generated sequential numbers
df_with_sno = df.withColumn("sno",monotonically_increasing_id())
display(df_with_sno)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Genrated unique id###
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import xxhash64

# Create Spark session
spark = SparkSession.builder.appName("HashIDExample").getOrCreate()

# Sample DataFrame
data = [("Alice", 23), ("Bob", 30), ("Cathy", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# Generate a unique hash ID using xxhash64 on name and age columns
df_with_hash = df.withColumn("unique_id",xxhash64("name","age"))

df_with_hash.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Regular expression it will suparate the name with number

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
# MAGIC ### Pivot##

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

# MAGIC %md
# MAGIC ###Formated NUMBER###

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, col

# Create Spark session
spark = SparkSession.builder.appName("FormatNumberExample").getOrCreate()

# Sample DataFrame with floating numbers
data = [("Alice", 12345.6789), ("Bob", 9876543.21), ("Cathy", 1000.5)]
columns = ["name", "salary"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)

# Format salary with commas and 2 decimal places
df_formatted = df.withColumn("formatted_salary", format_number(col("salary"), 2))

df_formatted.show(truncate=False)