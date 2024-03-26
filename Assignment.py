# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

class Assignment:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Assignment") \
            .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def create_product_table(self, path):
        df_product_raw = self.spark.read \
                         .format('csv') \
                         .option("header","true") \
                         .option("inferSchema","true") \
                         .load(path)
        for column in df_product_raw.columns:
            new_column_name = column.replace(" ", "_").lower()
            df_product_raw = df_product_raw.withColumnRenamed(column, new_column_name)
        return df_product_raw
    
    def create_customer_table(self, path):
        df_customer_raw = self.spark.read \
                      .format('com.crealytics.spark.excel') \
                      .option("header","true") \
                      .option("inferSchema","true") \
                      .load(path)
        for column in df_customer_raw.columns:
            new_column_name = column.replace(" ", "_").lower()
            df_customer_raw = df_customer_raw.withColumnRenamed(column, new_column_name)
        return df_customer_raw
    
    def create_order_table(self, path):
        df_order_raw = self.spark.read \
                   .format('json') \
                   .option("multiline","true") \
                   .option("inferSchema","true") \
                   .load(path)
        for column in df_order_raw.columns:
            new_column_name = column.replace(" ", "_").lower()
            df_order_raw = df_order_raw.withColumnRenamed(column, new_column_name)
        return df_order_raw
    
    def create_enriched_table(self, df_product, df_customer, df_order):
        df_product = df_product.select('product_id', 'category', 'sub-category')
        df_customer = df_customer.select('customer_id','customer_name', 'country')

        join_expr_1 = df_order['customer_id'] == df_customer['customer_id']
        join_expr_2 = df_order['product_id'] == df_product['product_id']

        df_enriched = df_order.join(df_customer, join_expr_1, "inner") \
                         .drop(df_customer['customer_id']) \
                         .join(df_product, join_expr_2,"inner") \
                         .drop(df_product['product_id']) \
                         .withColumn('profit',f.round(f.col('profit'),2))
        return df_enriched
    
    def create_aggregate_table(self, df_enriched):
        df_aggregate = df_enriched.withColumn('order_date', f.to_date(f.col('order_date'),"d/M/yyyy")) \
                            .withColumn('year',f.year(f.col('order_date'))) \
                            .select('profit','year','category','sub-category','customer_name')
        return df_aggregate
    
    def create_table(self, df, table_name):
        df.write.mode("overwrite").saveAsTable(table_name)

if __name__ == '__main__':
    product_path = "/FileStore/tables/Product.csv"
    product_table = "product"
    customer_path= "/FileStore/tables/Customer.xlsx"
    customer_table = "customer"
    order_path = "/FileStore/tables/Order.json"
    order_table = "order"
    enriched_table = "enriched_table"
    aggregate_table = "aggregate_table"

    try:
        assignment = Assignment()
        df_product = assignment.create_product_table(product_path)
        assignment.create_table(df_product, product_table)
        df_customer = assignment.create_customer_table(customer_path)
        assignment.create_table(df_customer, customer_table)
        df_order = assignment.create_order_table(order_path)
        assignment.create_table(df_order, order_table)
        df_enriched = assignment.create_enriched_table(df_product, df_customer, df_order)
        assignment.create_table(df_enriched, enriched_table)
        df_aggregate = assignment.create_aggregate_table(df_enriched)
        assignment.create_table(df_aggregate, aggregate_table)
    except Exception as e:        print(f"Error: {e}")