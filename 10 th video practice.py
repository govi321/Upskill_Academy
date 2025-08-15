# Databricks notebook source
# 1st Dataframe
data = [(1, "sravan", "TCS"),
        (2, "ojaswi", "WIPRO"),
        (3, "rohit", "CAPGEMINI"),
        (4, "sridevi", "COGNIZANT"),
        (5, "bobby", "ZENSAR")]

cols = ['ID', 'Name', 'Company']
employee = spark.createDataFrame(data, cols)
display(employee)


# COMMAND ----------

# 2nd Dataframe
data = [(1, "45000", "IT"),
        (2, "145000", "Manager"),
        (3, "45000", "HR"),
        (4, "34000", "Sales")]
        

cols = ['ID', 'salary', 'department']
department = spark.createDataFrame(data, cols)

display(department)

# COMMAND ----------

# MAGIC %md
# MAGIC #joins
# MAGIC df1.join(df2, df1.common_column == df2.df1.common_column, how = "left/right/left_anti/left_semi/left_outer/right_outer")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #inner join

# COMMAND ----------

employee.join(department,employee.ID==department.ID, how = "inner")
display(df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #left join

# COMMAND ----------

employee.join(department,employee.ID==department.ID, how = "left").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Right JOIN

# COMMAND ----------

employee.join(department,employee.ID==department.ID, how = "right").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Full outer JOIN

# COMMAND ----------


employee.join(department,employee.ID==department.ID, how = "full_outer").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #left_semi
# MAGIC .only left side record it's matched with right side record

# COMMAND ----------

employee.join(department,employee.ID==department.ID, how = "left_semi").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # left anti join
# MAGIC .only left side records doesn't matching with right side records 

# COMMAND ----------

employee.join(department,employee.ID==department.ID, how = "left_anti").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Create temparary view on Data frame ,if the session closed it will also terinated

# COMMAND ----------

data = [
    (7369, 'SMITH', 'CLERK', 7902, '17-DEC-1980', 800, 300, 20),
    (7499, 'ALLEN', 'SALESMAN', 7698, '20-FEB-1981', 1600, 300, 30),
    (7521, 'WARD', 'SALESMAN', 7698, '22-FEB-1981', 1250, 500, 30) ,
    (7566, 'JONES', 'MANAGER', 7839, '2-APR-1981', 2975,300, 20) ,
    (7654, 'MARTIN', 'SALESMAN', 7698, '28-SEP-1981', 1250, 1400, 30), 
    (7698, 'BLAKE', 'MANAGER', 7839, '1-MAY-1981', 2850, 500, 30),
    (7782, 'CLARK', 'MANAGER', 7839, '9-JUN-1981', 2450, 500, 10),
    (7788, 'SCOTT', 'ANALYST', 7566, '09-DEC-1982', 3000, 700, 20), 
    (7839, 'KING', 'PRESIDENT', 7566, '17-NOV-1981', 5000, 500, 10), 
    (7844, 'TURNER', 'SALESMAN', 7698, '8-SEP-1981', 1500, 700, 30), 
    (7876, 'ADAMS', 'CLERK', 7788, '12-JAN-1983', 1100, 300, 20), 
    (7900, 'JAMES', 'CLERK', 7698, '3-DEC-1981', 950, 500, 30), 
    (7902, 'FORD', 'ANALYST', 7566, '3-DEC-1981', 3000, 300, 20),
    (7934, 'MILLER', 'CLERK', 7782, '23-JAN-1982', 1300, 300, 10),
    
]

columns = ["EMPNO", "ENAME ","JOB","MGR","HIREDATE","SAL","COMM","DEPTNO"]

df = spark.createDataFrame(data, columns)
display(df)


# COMMAND ----------

df.createOrReplaceTempView("people_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people_view
# MAGIC

# COMMAND ----------

result = spark.sql("SELECT * FROM people_view WHERE EMPNO='7876'")
result.show()


# COMMAND ----------

result = spark.sql("SELECT * FROM people_view WHERE SAL>1500")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### #AVG salary departmentwise
# MAGIC

# COMMAND ----------

result = spark.sql("""
    SELECT DEPTNO, AVG(sal) AS avgsal
    FROM people_view
    GROUP BY DEPTNO
""")

result.show()



# COMMAND ----------

# MAGIC %md
# MAGIC #Total avg salary

# COMMAND ----------

result = spark.sql("""
    SELECT AVG(sal) AS total_avg_salary
    FROM people_view
""")

result.show()
