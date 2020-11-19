#!/usr/local/bin/python


import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from random import random
from operator import add
import os
import shutil

output=sys.argv[2]
input_file = sys.argv[1]
temporary_directory="/tmp/output/"

if (os.path.exists(temporary_directory)):
	shutil.rmtree(temporary_directory, ignore_errors=True)

os.makedirs(temporary_directory)

print("Starting data transformation...")
spark = SparkSession\
        .builder\
        .appName("split_records")\
        .getOrCreate()

df = spark.read.format("com.databricks.spark.xml") \
        .options(rowTag="PatientMatching") \
        .load(input_file)

df.printSchema()
print(df.head())

"""import uuid
id = str(uuid.uuid4())
df = df.withColumn("id", id)
df.show(truncate=False)
"""

df.write.format('com.databricks.spark.csv').mode('overwrite') \
	.option("header", "false").save(output)