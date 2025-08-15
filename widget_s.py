# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/govi/newfolder/")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/govi/Newfolder3",)

# COMMAND ----------

dbutils.fs.put("dbfs:/FileStore/tables/govi/newfolder/hello.csv","hello wolrd")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/newfolder/hello.csv")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/newfolder/helo.csv")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/govi/newfolder2/")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/govi/newfolder/hello.csv","dbfs:/FileStore/tables/govi/newfolder2")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/newfolder2/hello.csv")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/govi/Newfolder3")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/tables/govi/newfolder2/hello.csv","dbfs:/FileStore/tables/govi/Newfolder3")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/Newfolder3/hello.csv")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/newfolder2/employee.csv")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/govi")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/govi/Newfolder3/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/govi/newfolder2")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/Newfolder3/hello.csv")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/govi/Newfolder3/hello.csv")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/govi/Newfolder3")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/govi/Newfolder3")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

#create a text widget  with default value
dbutils.widgets.text("currentdate","12/08/2025")
#get the value entered in the text widget
dbutils.widgets.get("currentdate")


# COMMAND ----------

