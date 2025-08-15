# Databricks notebook source
# MAGIC %sql
# MAGIC create table EMP(ID int, NAME varchar(90), SALARY INT);
# MAGIC INSERT INTO EMP VALUES (1, 'DEEPAK', 10000), (2, 'SANJAY', 20000);
# MAGIC
# MAGIC SELECT * FROM EMP;

# COMMAND ----------

#create a schema
columns=["ID","NAME","SALARY"]
#create a data
data=[(1,'DEEPAK',10000),
      (2,'SANJAY',20000)
      ]
df= spark.createDataFrame(data=data,schema=columns)
df.show
display(df)

# COMMAND ----------

#read csv file with out header
df=spark.read.csv("dbfs:/FileStore/Newfolder/rahul.csv")
display(df)

# COMMAND ----------

#Reada the csv file with header

df=spark.read.csv("dbfs:/FileStore/Newfolder/rahul.csv",header = True)

display(df)

# COMMAND ----------

#Read the csv file with header


df=spark.read.option('header' ,'true').csv("dbfs:/FileStore/Newfolder/rahul.csv",header=True)
display(df)

# COMMAND ----------

df=spark.read\
.option('header','true')\
.option('multiline','true')\
.option('quote',"\"")\
.csv("dbfs:/FileStore/Newfolder/rahul.csv",header=True) 

display(df)

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/Newfolder3/myfile1_netsed.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

#read the json file
df=spark.read\
.option('header','true')\
.option('multiline','true')\
.option('quote',"\"")\
.json("dbfs:/FileStore/Newfolder3/myfile.json",)
df.show()

display(df)

# COMMAND ----------

#Read the parquet file
df=spark.read.parquet("dbfs:/FileStore/Newfolder4/userdata.parquet")
display(df)

# COMMAND ----------

#premmise
df=spark.read\
.option('header','true')\
.option('mode','permissive')\
.csv("dbfs:/FileStore/Newfolder5/premissive.csv")
display(df)

# COMMAND ----------

#dropmalformed
df=spark.read\
.option('header','true')\
.option('mode','DropMalformed')\
.csv("dbfs:/FileStore/Newfolder6/dropmalformed1.csv")

display(df)

# COMMAND ----------

#failfaster
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FailFastExample").getOrCreate()

df = spark.read.option("header", "true") \
               .option("mode", "FAILFAST") \
               .csv("dbfs:/FileStore/Newfolder6/dropmalformed1.csv")

df.show()