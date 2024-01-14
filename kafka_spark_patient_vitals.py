
# Import dependent libraries
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json




# Initiate Spark Session
spark = SparkSession  \
        .builder  \
        .appName("HealthApp")  \
        .enableHiveSupport() \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# Read Input Streams of Health Data
DF1 = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers", "localhost:9092")  \
        .option("subscribe", "Vital-Info")  \
        .option("startingOffsets", "earliest")  \
        .load()


# Define Health Schema
Schema = StructType() \
    .add("customerid", IntegerType()) \
    .add("heartbeat", IntegerType()) \
    .add("bp",IntegerType()) 
        

DF2 = DF1.select(from_json(col("value").cast("string"), Schema).alias("df")).select("df.*")


# Generate Health Stream with derived timestamp column
DF3 = DF2 \
       .withColumn("message_time",current_timestamp())	
       


# Save stream to hdfs storage in parquet format
DF3Storage = DF3 \
       .writeStream \
       .outputMode("append") \
       .format("parquet") \
       .option("truncate", "false") \
       .option("path","health-app/vital-info/") \
       .option("checkpointLocation","health-app/cp-vital-info/") \
       .trigger(processingTime="10 seconds") \
       .start()
       

DF3Storage.awaitTermination()
