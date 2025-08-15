# Databricks notebook source
# MAGIC %md
# MAGIC ## by using mkdir to create  a directory

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/New_folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ## by useing put we cam wrte some content in file 

# COMMAND ----------

dbutils.fs.put("dbfs:/FileStore/New_folder/hello.txt","this is my first file created in dbfs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## by using head we can see the content what ever it in the file

# COMMAND ----------

dbutil.fs.head("dbfs:/FileStore/New_folder/hello.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## By using he cp we can copy the file form one source to destination

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/New_folder/hello.txt","dbfs:/FileStore/New_folder1/copy_hello1.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ##BY using mv we can move the file from source to destination

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/New_folder/hello.txt","dbfs:/FileStore/Goverdhan_folder/hello.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## By using the rm we can delete the file  

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/New_folder1/hello1.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ##By using rmdir we can delete the empty directory 

# COMMAND ----------

dbutils.fs.rmdir("dbfs:/FileStore/New_folder")

# COMMAND ----------

# MAGIC %md
# MAGIC - ## By using rm and recurse = true we can delete the directoy 

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/New_folder1", recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to listout the file

# COMMAND ----------

#list out file
dbutils.fs.ls("dbfs:/FileStore")

# COMMAND ----------

#list out file
dbutils.fs.ls("dbfs:/FileStore/tables")