
from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.types import *




if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    df = spark.read.format("com.databricks.spark.xml") \
	    .options(rowTag="person") \
	    .load("/usr/local/airflow/dags/files/persons.xml")

    df.printSchema()
    print(df.head())

    spark.stop()
