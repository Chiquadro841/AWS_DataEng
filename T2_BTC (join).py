import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col, date_trunc, next_day, last


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


T1_BTC= "s3://crypto-silver-bucket/BTC.parquet"
T1_GT_bitcoin= "s3://crypto-silver-bucket/GT_bitcoin.parquet"

T2_output="s3://crypto-gold-bucket/final_BTC.parquet"


# Leggi i file Parquet da S3
BTC_df = spark.read.parquet(T1_BTC)
GT_bitcoin_df = spark.read.parquet(T1_GT_bitcoin)


# 1 - calcolo media a 10 giorni per il prezzo

windowSpec = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-9, 0)
# Calcola la media mobile a 10 sulla colonna Price
BTC_df = BTC_df.withColumn("MA10", F.avg("Price").over(windowSpec))


# 2 FARE JOIN TRA I DUE DATASET

joined_df = GT_bitcoin_df.join(BTC_df, GT_bitcoin_df["Settimana"] == BTC_df["Date"], "inner" ) \
                         .select(GT_bitcoin_df["Settimana"], "interesse_bitcoin", "Price", "MA10")

#togliamo i duplicati involuti
joined_df = joined_df.groupBy("Settimana").agg(
    F.first("Price").alias("Price"),
    F.first("MA10").alias("MA10")
)
joined_df.show(20)


# Salva il DataFrame risultante in S3 in formato Parquet
joined_df.write.mode("overwrite").parquet(T2_output)
