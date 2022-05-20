import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

def wrangle_311(spark):
    source = spark.read.csv('source.csv', sep=",",header=True,inferSchema=True)
    case = spark.read.csv('case.csv', sep=",",header=True,inferSchema=True)
    dept = spark.read.csv('dept.csv', sep=",",header=True,inferSchema=True)