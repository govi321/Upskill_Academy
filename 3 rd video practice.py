# Databricks notebook source
spark.sql ("create database kush")

# COMMAND ----------

spark.sql("create table kush.emp (eno int,ename varchar(90),esal int)" )
spark.sql("insert into kush.emp values(1,'vinod',999),(2,'nasik',777)")

# COMMAND ----------

display(spark.sql("select * from kush.emp"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC show databases

# COMMAND ----------

spark.sql ("show databases").show()

# COMMAND ----------

spark.sql ("create database vamshi")

# COMMAND ----------

spark.sql("show databases").show()


# COMMAND ----------

spark.sql("create table vamshi.dept(detno int, dename string)")
spark.sql("insert into vamshi.dept values (1, 'HR'), (2, 'DEV')")

# COMMAND ----------


spark.sql("insert into vamshi.dept values (1, 'HR'),(2,'DEV')")

# COMMAND ----------

spark.sql("select * from vamshi.dept").show()

# COMMAND ----------

spark.sql("select * from vamshi.dept").show()

# COMMAND ----------



# COMMAND ----------

spark.sql("create table kush.dept(detno int, dename string)")

# COMMAND ----------

spark.sql("select * from kush.dept").show()

# COMMAND ----------

spark.sql("insert into kush.dept values(1,'HR'),(2,'prod')")

# COMMAND ----------

spark.sql("select * from kush.dept").show()

# COMMAND ----------

display("select * from kush.dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kush.dept 

# COMMAND ----------


display(spark.sql("select * from kush.dept"))

# COMMAND ----------

spark.sql("select * from kush.km").show()

# COMMAND ----------

spark.sql("create table kush.km(gid int ,gname varchar(90))")

# COMMAND ----------

spark.sql("insert into kush.km values(10,'azure'),(20,'eee'),(30,'iti'),(40,'gcp')")

# COMMAND ----------

spark.sql("select * from kush.km").show()

# COMMAND ----------

spark.sql("create table vamshi.vs(gid int,gname varchar(90),gsal int ) ")

# COMMAND ----------

spark.sql("insert into vamshi.vs values(10,'santhosh',333),(20,'meghana',555),(30,'teja',777)")

# COMMAND ----------

spark.sql("select * from vamshi.vs").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## inner joins_

# COMMAND ----------

#display the km table data 

spark.sql("select * from kush.km").show()

#dsplay the vs table data

spark.sql("select * from vamshi.vs").show()


#join boyt table comman column  is gid

spark.sql("select * from kush.km as k inner join vamshi.vs as v on k.gid=v.gid ").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##left join

# COMMAND ----------

#display the km table data 

spark.sql("select * from kush.km").show()

#dsplay the vs table data

spark.sql("select * from vamshi.vs").show()


#join boyt table comman column  is gid

spark.sql("select * from kush.km as k left  join vamshi.vs as v on k.gid=v.gid ").show()

# COMMAND ----------

#display the km table data 

spark.sql("select * from kush.km").show()

#dsplay the vs table data

spark.sql("select * from vamshi.vs").show()


#join boyt table comman column  is gid

spark.sql("select * from kush.km as k inner  join vamshi.vs as v on k.gid=v.gid ").select('k.gid','k.gname','v.gsal').show()

# COMMAND ----------

#convert table to dataframe

df=spark.table("kush.km").collect()
display(df)

# COMMAND ----------

df1 = spark.table("vamshi.vs").collect()
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #import and export

# COMMAND ----------

# MAGIC %md
# MAGIC ## Go to File here we saw import and export then browser the file and select the file 
# MAGIC ## in export export the source file , and click on export this note book ## 