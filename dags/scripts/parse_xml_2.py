import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import *

FILE = ''

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Error: expected input parameters.')
    
    FILE = sys.argv[1]
    BUCKET_NAME = sys.argv[2]
    print('Processing file: {} from bucket {}'.format(FILE, BUCKET_NAME))

    
    spark = SparkSession.builder.appName("process_file").getOrCreate()
    sc=spark.sparkContext
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

    ACCESS_KEY_ID = "AKIA2VGYGUP2HWDXQVWM"
    SECRET_ACCESS_KEY = "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"

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")    
    hadoop_conf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.access.key", ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", SECRET_ACCESS_KEY)

    df = spark.read.format("com.databricks.spark.xml").option("rowTag", "PatientMatching") \
        .load("s3a://{}/{}".format(BUCKET_NAME, FILE))


    df.printSchema()
    print(df.head())

    spark.stop()
  
