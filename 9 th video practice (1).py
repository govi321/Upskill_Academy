# Databricks notebook source
#infer schema
df_adv=spark.read\
.option('header','true')\
.option('inferschema','true')\
.csv("dbfs:/FileStore/Newfolder7/coustomerdata.csv")
display(df_adv)
df_adv.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #Accessing the column multiple ways

# COMMAND ----------

df=df_adv.select ("FirstName","LastName")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Filtering the data using where/**filter**

# COMMAND ----------


from pyspark.sql.functions import col
df_adv.filter(col("MaritalStatus")=="M").show()
display(df_adv)    

# COMMAND ----------

# MAGIC %md
# MAGIC ##By using where

# COMMAND ----------

df_adv.where(df_adv.TotalChildren>2).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##BY USING 3 WAYS

# COMMAND ----------

from pyspark.sql.functions import col

#df_adv.filter(col("MaritalStatus")=="M").show()

df_adv.where(df_adv.MaritalStatus=="M").show()

#df_adv.where(df_adv.TotalChildren>2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove the , comm in my Annuall income

# COMMAND ----------

display(df_adv)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df_adv= df_adv.withColumn("NoComma", regexp_replace(col("AnnualIncome"), ",", ""))
display(df_adv)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Remove dollar $ only

# COMMAND ----------

df_adv = df_adv.withColumn("NoDoller",regexp_replace(col("AnnualIncome"), "\\$", ""))
display(df_adv)


# COMMAND ----------

# MAGIC %md
# MAGIC #select vs selectexpression

# COMMAND ----------


from pyspark.sql.functions import regexp_replace, col
df_adv.select ("FirstName",col("LastName")).show


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
#df_adv.select ("FirstName",col("LastName")).show
#select expression

df_adv.selectExpr("Firstname","cast(AnnualIncome as string )as IncomeString").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #sorting the data

# COMMAND ----------

display(df_adv.orderBy("AnnualIncome"))


# COMMAND ----------

# MAGIC %md
# MAGIC #descending order

# COMMAND ----------

display(df_adv.orderBy(df_adv.TotalChildren.desc()))


# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregation Group by

# COMMAND ----------

df_group =df_adv.groupBy("Gender").count()
display(df_group)

# COMMAND ----------

#drop duplictae
df_drop=df_adv.dropDuplicates(["AnnualIncome"])
display(df_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC #String AND Date Manipulation

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format
df_date=df_adv.withColumn("BirthDate_new",to_date("BirthDate", "yyyy-MM-dd"))

#if want to change existing format from yyyy-MM-dd to MM-dd-yyyy
df_format=df_adv.withColumn("Formateddate",date_format("BirthDate","dd--MM-yyyy"))

df_format.select("BirthDate","Formateddate","MaritalStatus").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## I want only Birth YEAR

# COMMAND ----------


from pyspark.sql.functions import to_date,date_format,year,month,days,col
df_year=df.withColumn("Birthyear",year(col("BirthDate")))
display(df_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## I want only Birth month

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format,year,month,days,col
df_year=df_adv.withColumn("Birthmonth",month(col("BirthDate")))
display(df_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling SPRING MANIPILATION INSPARK
# MAGIC .UPPER CASE AND LOWER CASE

# COMMAND ----------

from pyspark.sql.functions import upper,lower
df_str = df.withColumn("FirstName_Name", lower(col("FirstName")))
display(df_str)

# COMMAND ----------

# MAGIC %md
# MAGIC . upper case
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import upper,lower
df_str = df.withColumn("FirstName_Name", upper(col("FirstName")))
display(df_str)

# COMMAND ----------

from pyspark.sql.functions import upper,lower
df_str = df_adv.withColumn("Occupation_as_upper", upper(col("Occupation")))
display(df_str)

# COMMAND ----------

# MAGIC %md
# MAGIC #**union  and  union all

# COMMAND ----------

# Create two DataFrames with duplicate rows
df1 = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob")
], ["id", "name"])

df2 = spark.createDataFrame([
    (2, "Bob"),
    (3, "Charlie")
], ["id", "name"])

# UNION ALL (duplicates preserved)
#df_union_all = df1.union(df2)
df_union_distinct = df1.union(df2).distinct()


#df_union_all.show()
df_union_distinct.show()
