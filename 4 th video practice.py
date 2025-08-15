# Databricks notebook source

schema=["eno","ename","esal","age"]

data=[(1,'vihnash',888,25),
     (2,'gunni',777,55),
     (3,'fury',666,45),
     (4,'sunil',222,65)
    ]



df=spark.createDataFrame(data=data,schema=schema)

df.show()
display(df)



# COMMAND ----------

empcolumns=["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]


empdata=[(1,"smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",3000),
      (3,"Williams",1,"2010","10","M",3000),
      (4,"Jones",2,"2005","10","F",3000),
      (5,"Brown",2,"2010","40","",3000),
      (6,"Brown",2,"2010","50","",3000),
]




empDF=spark.createDataFrame(data=empdata,schema=empcolumns)

empDF.show()
empDF.display()
empDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##add the new column to table by using wothColumn

# COMMAND ----------

df=empDF.withColumn("updatesalary",empDF.salary+1500) 

display(df)
df.printSchema()

# COMMAND ----------

df1= df.withColumnRenamed("updatesalary","Amount")

display(df1)

df1 =df.withColumnRenamed("updatesal","Amount")

display(df1)




# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

y= StructType([\
   StructField("eno",IntegerType(),True),
   StructField("ename",StringType(),True),
   StructField("esal",IntegerType(),True)
])

x= [(1,'sandya',777),
    (2,'vindy',666),
    (3,'gunni',555)
]

df2=spark.createDataFrame(data=x,schema=y)

display(df2 )
df2.printSchema()

# COMMAND ----------

df4=df2.withColumn("esal",when(col("eno")==2, 888).otherwise(col("esal"))) 
df4.show()                

# COMMAND ----------

from pyspark.sql.functions import *

df3=df2.withColumn("date",current_date())

df3=df2.withColumn("date",current_date()).withColumn("currenttime",current_timestamp())

display(df3)

# COMMAND ----------

