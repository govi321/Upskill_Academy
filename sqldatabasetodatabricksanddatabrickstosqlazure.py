# Databricks notebook source

jdbcUsername = "sqlserveradmin"
jdbcPassword = "admi123!@#"
jdbcHostname = "vardhansqlserver.database.windows.net" # hostname is servername
jdbcDatabase = "vardhasqldatabse"
jdbcPort = 1433

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"
print(jdbcUrl)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read the sql query in azure sqldatabse though data brics

# COMMAND ----------


df1= spark.read.jdbc(url=jdbcUrl, table = "customers", properties={"user": "sqlserveradmin", "password": "admin123!@#"})
display(df1)

# COMMAND ----------

df_fileter=df1.filter(df1.City == "Mumbai")
display(df_fileter)

# COMMAND ----------

df1= spark.read.jdbc(url=jdbcUrl, table = "sales", properties={"user": "sqlserveradmin", "password": "admin123!@#"})
display(df1)

# COMMAND ----------

df_fileter=df1.filter(df1.ProductName == "Laptop Stand")
display(df_fileter)

# COMMAND ----------

df1= spark.read.jdbc(url=jdbcUrl, table = "orders", properties={"user": "sqlserveradmin", "password": "admin123!@#"})
display(df1)

# COMMAND ----------

df_fileter=df1.filter(df1.OrderID == "1001")
display(df_fileter)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing a dataframe back to azure sql `database`

# COMMAND ----------

    # Define connection properties
    jdbcHostname = "vardhansqlserver.database.windows.net"
    jdbcDatabase = "vardhasqldatabse"
    jdbcPort = 1433
    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

    connectionProperties = {
      "user": "sqlserveradmin",
      "password": "admin123!@#",
      "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Read data from Azure SQL Database into a DataFrame
    df = spark.read.jdbc(url=jdbcUrl, table="orders", properties=connectionProperties)

    # Display the DataFrame
    df.display()

    # To write data back:
    # df.write.jdbc(url=jdbcUrl, table="<target-table-name>", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###filtering the data

# COMMAND ----------

df4=df.filter(df.OrderID == "1002")
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### send back to data to azure data base

# COMMAND ----------

 # To write data back:
 df4.write.jdbc(url=jdbcUrl, table="filter_order", mode="overwrite", properties=connectionProperties)