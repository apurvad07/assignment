# Databricks notebook source
spark.catalog.listTables()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select round(sum(profit), 2) as total_profit, year from aggregate_table group by year;

# COMMAND ----------

# MAGIC %sql
# MAGIC select round(sum(profit), 2) as total_profit, year, category as product_category from aggregate_table group by year, category;

# COMMAND ----------

# MAGIC %sql
# MAGIC select round(sum(profit), 2) as total_profit, customer_name from aggregate_table group by customer_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select round(sum(profit), 2) as total_profit, customer_name, year from aggregate_table group by customer_name, year;