from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType, FloatType, DoubleType
from pyspark.sql.functions import col , explode

print('------------------------------------------------------version :- 1.2---------------------------------------------------')
spark = SparkSession.builder \
    .appName("Flattening") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYUPL43FR4PJEDLNG") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .getOrCreate()
df1 = spark.read.option("multiline",'true').json("/opt/spark/work-dir/orders_etl.json")

df3 = df1.select("*", explode("products").alias("product"))


df4 = df3.select('order_id', 'order_date', 'total_amount', 'customer.*', 'product')
print("----------------------------------DF4---------------------------")
df4.printSchema();
df4.show(truncate=False)

df4.write.mode("overwrite").parquet("s3a://serverless-etl-project-manas/raw")


print("-------------------------------------version 1.2 completed-----------------------------")

