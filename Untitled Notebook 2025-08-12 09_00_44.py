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

df2=df.dropDuplicates(["AnnualIncome"])
display(df2)