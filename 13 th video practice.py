# Databricks notebook source
# MAGIC %scala
# MAGIC val df = spark.read.option("header", "true").csv("/mnt/rawdata/sales.csv")
# MAGIC df.show()
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC # Create a simple vector and calculate mean
# MAGIC numbers <- c(10, 20, 30, 40, 50)
# MAGIC mean(numbers)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### This is my note book
# MAGIC **this my line**
# MAGIC . one more text

# COMMAND ----------

# MAGIC %md
# MAGIC #this is
# MAGIC
# MAGIC ![](https://cognitivetoday.com/wp-content/uploads/2012/08/Big-data-Different.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC %run
# MAGIC ##### I want to run the 1.Notebook code to 2.Note book code we use % run  
# MAGIC
# MAGIC %fs
# MAGIC #### fs is the file system
# MAGIC
# MAGIC %pip
# MAGIC ### python installation

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data bricks widgets
# MAGIC .Textbox widgets

# COMMAND ----------

dbutils.widgets.text(name='FruitText', defaultValue='Apple', label='TextBox')
value = dbutils.widgets.get('FruitText')
print("value")


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Combobox`

# COMMAND ----------

dbutils.widgets.combobox(name="Fruitecombobox", defaultValue="Apple", choices=["Apple", "Orange", "Banana", "Pear"], label="combo")

value = dbutils.widgets.get("Fruitecombobox")
print(value)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Down

# COMMAND ----------


dbutils.widgets.dropdown(name="Fruitedropdown", defaultValue="Apple", choices=["Apple", "Orange", "Banana", "Pear"], label="dropdown")

value = dbutils.widgets.get("Fruitedropdown")
print(value)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Multiselect

# COMMAND ----------

dbutils.widgets.multiselect(name="Fruitemultiselect", defaultValue="Apple", choices=["Apple", "Orange", "Banana", "Pear"], label="multiselect")

value = dbutils.widgets.get("Fruitemultiselect")
print(value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample widget file

# COMMAND ----------

df_widget = spark.read.option('header', 'true')\
    .option('inferSchema', 'true')\
    .csv('dbfs:/FileStore/tables/widgets.csv')

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
# make sure df_widget is your actual DataFrame name

filtered_df = df_widget

if country:
    filtered_df = filtered_df.filter(col("Country") == country)

if product:
    filtered_df = filtered_df.filter(col("Product") == product)

if order_date:
    filtered_df = filtered_df.withColumn("OrderDate", to_date("OrderDate")) \
                             .filter(col("OrderDate") >= to_date(lit(order_date)))
if product:
    filtered_df = filtered_df.filter(col("Product") == product)

if order_date:
    filtered_df = filtered_df.withColumn("OrderDate", to_date("OrderDate")) \
                             .filter(col("OrderDate") >= to_date(lit(order_date)))

if departments and departments != ['']:
    filtered_df = filtered_df.filter(col("Department").isin(departments))

# Step 4: Display result
display(filtered_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##dbutils

# COMMAND ----------

dbutils.help()


# COMMAND ----------

# MAGIC %md
# MAGIC ##File systemhelp

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use help

# COMMAND ----------

help(spark.read.csv)