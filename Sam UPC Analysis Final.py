# Databricks notebook source
import upc_input
from kpi_metrics import KPI
import pyspark.sql.functions as f

# COMMAND ----------

kpi = KPI(spark, use_sample_mart=True)

# COMMAND ----------

#Read in upc list provided to us by client
user_id = "s925140"
upc_list = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema", True).csv(f'dbfs:/Users/{user_id}@8451.com/ROOTS_UPC.csv')
display(upc_list)

# COMMAND ----------

upc_input.gather_match_stats(
    kpi=kpi,
    df=upc_list,
    upc_col='bas_con_upc_no')

# COMMAND ----------

all_the_upcs = upc_input.review_upcs(
    kpi=kpi,
    upc_df = upc_list,
    match_type = 'scan', #Matching off scan since we had more matches
    upc_col = 'bas_con_upc_no',
    start_date = '20210131', #Fiscal year 2021 start date
    end_date = '20220129' #Fiscal year 2021 end date
)

# COMMAND ----------

#showing the 3 upcs that didn't match against scan upcs. 3 is insignificant so we don't need to worry about them.
all_the_upcs[0].filter(f.col("con_upc_no").isNull()).display()

# COMMAND ----------


