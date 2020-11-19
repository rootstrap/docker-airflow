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

schema = StructType([
    StructField('TAGS', StringType(), True),
    StructField('TEXT', StringType(), True)
    ])
"""StructField('ADVANCED-CAD', StringType(), True),
StructField('ALCOHOL-ABUSE', StringType(), True),
StructField('ASP-FOR-MI', StringType(), True),
StructField('CREATININE', StringType(), True),
StructField('DIETSUPP-2MOS', StringType(), True),
StructField('DRUG-ABUSE', StringType(), True),
StructField('ENGLISH', StringType(), True),
StructField('HBA1C', StringType(), True),
StructField('KETO-1YR', StringType(), True),
StructField('MAJOR-DIABETES', StringType(), True),
StructField('MAKES-DECISIONS', StringType(), True),
StructField('MI-6MOS', StringType(), True),"""

df = spark.read.format("com.databricks.spark.xml") \
        .options(rowTag="PatientMatching") \
        .load(input_file, schema=schema)

df.printSchema()
print(df.show())

"""import uuid
id = str(uuid.uuid4())
df = df.withColumn("id", id)
df.show(truncate=False)
"""

df.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(temporary_directory)

if (os.path.exists(temporary_directory + "_SUCCESS")):
	print("Removing file " + temporary_directory + "_SUCCESS")
	os.remove(temporary_directory + "_SUCCESS")	
	files = [f for f in os.listdir(temporary_directory)]
	files = list(filter(lambda f: f.endswith('.csv'), files))
	if len(files) > 1:
		print("More than one file has been generated. Total csv files {}.", len(files))
	else:
		print("Renaming file from {} to {}".format(files[0], output))
		os.rename(temporary_directory + files[0], output)
