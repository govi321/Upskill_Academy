# Databricks notebook source
# MAGIC %sql
# MAGIC create table EMP(ID int, NAME varchar(90), SALARY INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO EMP VALUES (1, 'DEEPAK', 10000), (2, 'SANJAY', 20000);
# MAGIC select * from emp 

# COMMAND ----------

columns=["ID","NAME","SALARY"]
#create a data
data=[(1,'DEEPAK',10000),
      (2,'SANJAY',20000)
      ]
df= spark.createDataFrame(data=data,schema=columns)
df.show
display(df)

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/govi/newfolder2/employee.csv")
display(df)

# COMMAND ----------

df =spark.read.csv("dbfs:/FileStore/tables/govi/newfolder2/employee.csv",header=True)
display(df)

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/govi/newfolder2/employee.csv",header=True)
display(df)

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/FileStore/tables/govi/newfolder2/employee.csv")
display(df)

# COMMAND ----------

df =spark.read.option('header','true').csv("dbfs:/FileStore/tables/govi/newfolder3/multiline.csv")
display(df)

# COMMAND ----------

df =spark.read\
.option('header','true')\
.option('multiline','true')\
.option('quote',"\"")\
.csv("dbfs:/FileStore/tables/govi/newfolder3/multiline.csv")
display(df)       

# COMMAND ----------

df=spark.read\
.option('header','true')\
.option('multiline','true')\
.option('quote',"\"")\
.json("dbfs:/FileStore/tables/govi/newfolder3/employeejsonfile.json")
df.show()
    



# COMMAND ----------



df=spark.read\
.option('header','true')\
.option('multiline','true')\
.option('quote',"\"")\
.json("dbfs:/FileStore/tables/govi/newfolder3/myfile1_netsed.json")
df.show()



# COMMAND ----------

df =spark.read.parquet("dbfs:/FileStore/tables/govi/newfolder3/userdata.parquet")
display(df)

# COMMAND ----------

df =spark.read\
    .option('header','true')\
    .option('mode','permissive')\
    .csv("dbfs:/FileStore/tables/govi/newfolder2/premissive.csv")
display(df)    

# COMMAND ----------

df =spark.read\
    .option('header','true')\
    .option('mode','DROPMALFORMED') \
        .csv("dbfs:/FileStore/tables/govi/newfolder2/dropmalformed.csv")
display(df)

# COMMAND ----------

df =spark.read\
    .option('header','true')\
    .option('mode','FAILFAST')\
    .csv("dbfs:/FileStore/tables/govi/newfolder2/dropmalformed.csv")
display(df)        

# COMMAND ----------

df =spark.read\
    .option('header','true')\
    .option('inferschema','true')\
    .csv("dbfs:/FileStore/tables/govi/newfolder/coustomers.csv") 
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
df.select("FirstName").show()



# COMMAND ----------

from pyspark.sql.functions import col
df.select("FirstName",col("LastName")).show()

# COMMAND ----------

df.selectExpr("FirstName","cast(AnnualIncome as string) as IncomeString").show()

# COMMAND ----------

display(df.orderBy("AnnualIncome"))

# COMMAND ----------

display(df.orderBy(df.TotalChildren.desc()))

# COMMAND ----------

df1=df.groupBy("MaritalStatus").count()
display(df1)        

# COMMAND ----------


from pyspark.sql.functions import col
df2 =df.filter(col("MaritalStatus")== "M").show()
display(df2)

# COMMAND ----------

df2 =df.filter(col("MaritalStatus")== "S").show()
display(df2)


# COMMAND ----------

df2= df.filter(df.MaritalStatus=="M").show()
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col
d2=df.where(col('maritalstatus')=="M").show()


# COMMAND ----------

df2=df.dropDuplicates(["AnnualIncome"])
display(df2)

# COMMAND ----------




# COMMAND ----------

from pyspark.sql.functions import col,regexp_replace
#regexp_replace( threee parameters)
df2=df.withColumn("No_Dollar",regexp_replace(col("AnnualIncome"),"\\$",""))
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col
df2=df.select("FirstName","LastName")
display(df2)

# COMMAND ----------

df.selectExpr("FirstName", "cast(AnnualIncome as string) as incomeString").show()

# COMMAND ----------

display(df.orderBy("AnnualIncome"))

# COMMAND ----------

df.orderBy(df.TotalChildren.desc()).show()

# COMMAND ----------

df2 =df.groupBy("MaritalStatus").count()
display(df2)

# COMMAND ----------

df2 =df.dropDuplicates(["AnnualIncome"])
display(df2)

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format
df2=df.withColumn("NewBirthDay",to_date("BirthDate","yyyy-MM-dd")) 
display(df2)

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format
df2=df.withColumn("Formatedate",date_format("BirthDate","dd-MM-yyyy")) 
display(df2)

# COMMAND ----------

df2.select("BirthDate","FormateDate","MaritalStatus").show()

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format,year,month,days,col

df2= df.withColumn("Birthyear",year(col("BirthDate")))
display(df2)


# COMMAND ----------

from pyspark.sql.functions import lower ,upper
df3 =df2.withColumn("FirstName",lower(col("FirstName")))
display(df3)


# COMMAND ----------

df2=df.withColumn("Occupation_upper",upper(col("Occupation")))
display(df2)

# COMMAND ----------

# 1st DataFrame
data = [
    [1, "sravan", "TCS"],
    [2, "ojaswi", "WIPRO"],
    [3, "rohith", "CAPGEMINI"],
    [4, "sridevi", "COGNIZANT"],
    [5, "bobby", "ZENSAR"]
]

cols = ['ID', 'Name', 'Company']

employee = spark.createDataFrame(data, cols)

display(employee)


# COMMAND ----------

# 2nd DataFrame
data = [
    [1, "45000", "IT"],
    [2, "145000", "Manager"],
    [3, "45000", "HR"],
    [4, "34000", "Sales"]
]

cols = ['ID', 'salary', 'department']

department = spark.createDataFrame(data, cols)

display(department)


# COMMAND ----------

left_join_df=employee.join(department,employee.ID==department.ID,how="left")
display(left_join_df)

# COMMAND ----------

Right_join_df=employee.join(department,employee.ID==department.ID, how="right")
display(Right_join_df)

# COMMAND ----------

full_join_df=employee.join(department,employee.ID==department.ID,how="fullouter")
display(full_join_df)

# COMMAND ----------

inner_join_df=employee.join(department,employee.ID==department.ID,"inner")

display(inner_join_df)

# COMMAND ----------

left_semi_df=employee.join(department,employee.ID==department.ID, "Left_semi")
display(left_semi_df)

# COMMAND ----------

left_anti_df=employee.join(department,employee.ID==department.ID,"left_anti")
display(left_anti_df)

# COMMAND ----------

# Sample data
data = [
    (1, "John", "Doe", "Engineering", 70000),
    (2, "Jane", "Smith", "HR", 50000),
    (3, "Mike", "Johnson", "Finance", 60000),
    (4, "Emily", "Davis", "Engineering", 75000),
    (5, "Robert", "Brown", "Marketing", 55000),
    (6, "Linda", "Wilson", "HR", 52000)
]

# Column names
columns = ["ID", "FIRST_NAME", "LAST_NAME", "DEPARTMENT", "SALARY"]

# Create DataFrame
df_dept = spark.createDataFrame(data, columns)

# Show the DataFrame
display(df_dept)


# COMMAND ----------

df_dept.createOrReplaceTempView("employee")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee where salary="70000"

# COMMAND ----------

spark.sql("select * from employee where DEPARTMENT='Engineering'").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select ID,First_Name,LAST_NAME,SALARY from employee where salary >70000 

# COMMAND ----------

spark.sql("select ID,First_Name,LAST_NAME,SALARY from employee where salary >65000").show


# COMMAND ----------

spark.sql(""" select DEPARTMENT,avg(salary)as avgsal from employee group by DEPARTMENT""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##markdown
# MAGIC **THIS IS MARKDOWN**

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://static.vecteezy.com/system/resources/thumbnails/008/405/918/small/big-data-analytics-big-data-innovation-technology-concept-blockchain-network-analysis-artificial-intelligence-hexagonal-grid-sphere-in-wireframe-hand-on-blue-background-illustration-vector.jpg)

# COMMAND ----------

dbutils.widgets.text(name='FruitText',defaultValue='apple',label='TextBox')
value=dbutils.widgets.get('FruitText')
print(value)

# COMMAND ----------

dbutils.widgets.combobox(name='FruitCombobox',defaultValue='Apple',choices= ['Apple','Bannana','Orange','pear'],label='combo')
value=dbutils.widgets.get("FruitCombobox")  
print(value)                      

# COMMAND ----------

dbutils.widgets.dropdown(name='dropdown', defaultValue='Apple', choices=['Apple', 'Bannana', 'Orange', 'pear'])
value=dbutils.widgets.get("dropdown")
print(value)

# COMMAND ----------

df_widget=spark.read\
    .option('header','true')\
    .option('inferschema','true')\
    .csv('dbfs:/FileStore/tables/govi/newfolder/widgets.csv')
display(df_widget)    

# COMMAND ----------

from pyspark.sql.functions import col, to_date, lit

# Step 1: Create widgets
dbutils.widgets.text("OrderDate", "", "Order Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("Country", "India", ["USA", "India", "UK", "Germany", "France"])
dbutils.widgets.combobox("Product", "Laptop", ["Laptop", "Mouse", "Keyboard", "Desk", "Chair", "Monitor", "Table", "Notebook", "Pen", "Mousepad", "Printer"])
dbutils.widgets.multiselect("Department", "Electronics", ["Electronics", "Furniture", "Stationery"])

# Step 2: Read widget values
order_date = dbutils.widgets.get("OrderDate")
country = dbutils.widgets.get("Country")
product = dbutils.widgets.get("Product")
departments = dbutils.widgets.get("Department").split(",")

# Step 3: Start filtering
filtered_df = df_widget  # make sure df_widget is your actual DataFrame name

if country:
    filtered_df = filtered_df.filter(col("Country") == country)

if product:
    filtered_df = filtered_df.filter(col("Product") == product)

if order_date:
    filtered_df = filtered_df.withColumn("OrderDate", to_date("OrderDate")) \
                             .filter(col("OrderDate") >= to_date(lit(order_date)))
if departments and departments != ['']:
    filtered_df = filtered_df.filter(col("Department").isin(departments))

# Step 4: Display result
display(filtered_df)
