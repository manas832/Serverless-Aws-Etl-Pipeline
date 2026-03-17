from pyspark.sql import SparkSession
from pyspark.sql.functions import col 
import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#----------------------------------------------AWS Glue Job Parameters--------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



#----------------------------------------------------------------
spark = SparkSession.builder \
    .appName("transformations") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYUPL43FR4PJEDLNG") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .getOrCreate()

df1 = spark.read.parquet("s3a://serverless-etl-project-manas/raw")
print('-------------------------------------------customers_df--------------------------------------')
customers_df = df1.select( col('customer_id'), col('name').alias('customer_name'), col('email').alias('customer_email'), col('address').alias('customer_address') )
customers_df.show()
customers_df.write.parquet("s3a://serverless-etl-project-manas/processed/customers_df", mode="overwrite");
print("-------------------------------------------orders_df-------------------------------------- ")
orders_df = df1.select(col('order_id'), col('customer_id'), col('order_date'), col('total_amount'))
orders_df.show()
orders_df.write.parquet("s3a://serverless-etl-project-manas/processed/orders_df", mode="overwrite");
print("-------------------------------------------products_df-------------------------------------- ")
products_df = df1.select('product.product_id', col('product.name').alias('product_name'), col('product.category').alias('product_category'), col('product.price').alias('product_price') , col('product.quantity').alias('product_quantity'), col('order_id'), col('customer_id'))
products_df.show()
products_df.write.parquet("s3a://serverless-etl-project-manas/processed/products_df", mode="overwrite");


job.commit()

spark.stop();

