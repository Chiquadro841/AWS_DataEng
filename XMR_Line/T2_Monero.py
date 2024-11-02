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


T1_XMR= "s3://crypto-silver-bucket/XMR.parquet"
T1_GT_monero= "s3://crypto-silver-bucket/GT_monero.parquet"

T2_output="s3://crypto-gold-bucket/final_XMR.parquet"


# Leggi i file Parquet da S3
XMR_df = spark.read.parquet(T1_XMR)
GT_monero_df = spark.read.parquet(T1_GT_monero)


# 1 - calcolo media a 10 giorni per il prezzo

windowSpec = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-9, 0)
# Calcola la media mobile a 10 sulla colonna Price
XMR_df = XMR_df.withColumn("MA10", F.avg("Price").over(windowSpec))


# 2 FARE JOIN TRA I DUE DATASET

joined_df = GT_monero_df.join(XMR_df, GT_monero_df["Settimana"] == XMR_df["Date"], "inner" ) \
                         .select(GT_monero_df["Settimana"], "Monero_interesse", "Price", "MA10")

#togliamo i duplicati involuti
joined_df = joined_df.groupBy("Settimana").agg(
    F.first("Price").alias("Price"),
    F.first("MA10").alias("MA10"),
    F.first("Monero_interesse").alias("Monero_interesse")
)
joined_df.show(20)


# Salva il DataFrame risultante in S3 in formato Parquet
joined_df.write.mode("overwrite").parquet(T2_output)
