# Databricks notebook source
#Read in data 

# COMMAND ----------

user_id= "m723276"

dbutils.fs.cp(f'dbfs:/FileStore/Users/{user_id}@8451.com/ROOTS_UPC.csv', f'dbfs:/Users/{user_id}@8451.com/')

dbutils.fs.ls(f'dbfs:/Users/{user_id}@8451.com/')

# COMMAND ----------

upc = spark.read.option("header","true").csv(f'dbfs:/Users/{user_id}@8451.com/ROOTS_UPC.csv')
