
import numpy as np
import json
import collections
import requests
import time
import tqdm
import pandas as pd
import xmltodict
from operator import add

# own helper library
import utils

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf, avg, col, desc
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

t_wait = 0.005 # waiting time, to potentially avoid to be detected as an attacker...
data_total = []

# fields extracted
ks_data = ['identifier','timestamp','authors','orci_authors','typology'] #,'title','subject_list']

# Initial data download
url_bielfeld = 'http://pub.uni-bielefeld.de/oai?verb=ListRecords&metadataPrefix=oai_datacite'
r = requests.get(url_bielfeld)
o = xmltodict.parse(r.content)

data = o['OAI-PMH']['ListRecords']['record'] 
r_token = o['OAI-PMH']['ListRecords']['resumptionToken']['#text']
tmp_data = [utils.get_data_dict(d, ks_data) for d in data]
#df = spark.createDataFrame([[d[k] for k in ks_data] for d in tmp_data], 
#                            ks_data)
#tmp_data = []
df = None

# Loop over all the data as long as resumption token found...
while True:
    url_bielfeld = 'https://pub.uni-bielefeld.de/oai?verb=ListRecords&resumptionToken='+r_token
    r = requests.get(url_bielfeld)
    o = xmltodict.parse(r.content)
    data = o['OAI-PMH']['ListRecords']['record'] 
    tmp_data += [utils.get_data_dict(d, ks_data) for d in data]
    
    if not o['OAI-PMH']['ListRecords']['resumptionToken'].get('#text', False):
        print('********** data loaded **********')
        break

    r_token = o['OAI-PMH']['ListRecords']['resumptionToken']['#text']
    
    if len(tmp_data) % 1000 == 0:
        
        df_tmp = spark.createDataFrame([[d[k] for k in ks_data] for d in tmp_data], 
                                       ks_data)
        if df is None:
            df = df_tmp
        else:
            df = df.union(df_tmp)
        tmp_data = []
        print(df.count())

df_tmp = spark.createDataFrame([[d[k] for k in ks_data] for d in tmp_data], ks_data)
df = df.union(df_tmp)    

# Show dataframe
df.show()
print('df count total: ', df.count())
    

print('The number of records per publication year:')
print('************************************************************')

df.groupby(df.timestamp).count().sort('timestamp', ascending=True).show()


print('The number of records per typology:')
print('************************************************************')

df.groupby(df.typology).count().sort('typology', ascending=True).show()


print('Number of records per author:')
print('************************************************************')
dd = df.select('authors').rdd. \
        flatMap(lambda x: [(d,1) for d in x[0]]).reduceByKey(add)
for d in sorted(dd.collect(), key=lambda x: x[1], reverse=True)[1:10]:
    print(d)
    

print('Number of records per ORCI author:')
print('************************************************************')
dd = df.select('orci_authors').rdd. \
        flatMap(lambda x: [(d,1) for d in x[0]]).reduceByKey(add)
for d in sorted(dd.collect(), key=lambda x: x[1], reverse=True)[1:10]:
    print(d)


print('The number of Journal Articles produced since 1985, grouped by intervals of 5 years:')
print('************************************************************')

def process_year(y):
    for x in range(1985, 2021, 5):
        if x <= int(y) <= x+5:
            return str(str(x)+'<=y<='+str(x+5))

data_tmp = df.select('timestamp').rdd.flatMap(lambda y: [(process_year(y[0]), 1)]).reduceByKey(add)
for d in sorted(data_tmp.collect(), key=lambda x: x[0]):
    print(d)
