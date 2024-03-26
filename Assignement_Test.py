# Databricks notebook source
# MAGIC %run "./Assignment"

# COMMAND ----------

from unittest import *
from pyspark.sql import SparkSession

class Assignment_Test(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .appName("Assignment_Test") \
            .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
            .getOrCreate()
        cls.assignment = Assignment()

    def test_aggregate_columns(self):
        df_enriched = self.spark.read.table("enriched_table")
        column_list = ['profit','year','category','sub-category','customer_name']
        df_aggregate = self.assignment.create_aggregate_table(df_enriched)
        assert all([col in df_aggregate.columns for col in column_list])



# COMMAND ----------

def suite():
    suite = TestSuite()
    suite.addTest(Assignment_Test('test_aggregate_columns'))
    return suite

# COMMAND ----------

runner = TextTestRunner()
runner.run(suite())