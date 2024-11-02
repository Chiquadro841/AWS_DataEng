import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Configurazione della connessione a Redshift (credenziali in chiaro per semplicità invece di SecretsManager)
redshift_connection_options = {

    "url":"jdbc:redshift://cypto-spacenames.626635429853.eu-north-1.redshift-serverless.amazonaws.com:5439/dev",
    "user": "admin",
    "password": "JW28krhps",
    "dbtable": "public.bitcoin",
    "redshiftTmpDir": "s3://crypto-gold-bucket/",  # S3 bucket per i dati temporanei
}

# Percorso Parquet in S3
T2_input = "s3://crypto-gold-bucket/final_BTC.parquet"

# Leggi il file Parquet da S3 per caricarlo su redshift
df = spark.read.parquet(T2_input)

df.show(20)

# Scrivi i dati in Redshift
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df, glueContext, "dynamic_df"),  # inserimento come DynamicFrame
    connection_type="redshift",
    connection_options=redshift_connection_options,
    transformation_ctx="datasink"
)
