
import numpy as np
import pandas as pd
import json
import collections

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf, avg, col, desc
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


# Create a Spark DataFrame from a Pandas DataFrame using Arrow
df = spark.createDataFrame(pd.read_csv('data/df.csv'))


print('The number of records per publication year:')
print('************************************************************')

df.groupby(df.year).count().sort('year', ascending=True).show()


print('The number of records per typology:')
print('************************************************************')

df.groupby(df.typology).count().sort('typology', ascending=True).show()


print('Number of records per author:')
print('************************************************************')

data_tmp = df.select('authors').rdd. \
        flatMap(lambda x: [[str(d)] for d in json.loads(x[0])]).filter(lambda x: x!=['[]'])

cSchema = StructType([StructField('authors', StringType())])
spark.createDataFrame(data_tmp.collect(),cSchema). \
    groupby('authors').count().sort(col('count').desc()).show() 


print('Number of records per ORCID identified author:')
print('************************************************************')

data_tmp = df.select('orci_authors').rdd. \
        flatMap(lambda x: [[str(d)] for d in json.loads(x[0])]).filter(lambda x: x!=['[]'])

cSchema = StructType([StructField('orci_authors', StringType())])
spark.createDataFrame(data_tmp.collect(),cSchema). \
    groupby('orci_authors').count().sort(col('count').desc()).show() 


print('The number of Journal Articles produced since 1985, grouped by intervals of 5 years:')
print('************************************************************')

def process_year(y):
    for x in range(1985, 2021, 5):
        if x <= y <= x+5:
            return str(x)+'<=y<='+str(x+5)

cSchema = StructType([StructField('years_group', StringType())])
data_tmp = df.select('year').rdd.map(lambda y: [str(process_year(y[0]))])

spark.createDataFrame(data_tmp.collect(), cSchema). \
    groupby('years_group').count().sort(col('years_group').asc()).show() 

