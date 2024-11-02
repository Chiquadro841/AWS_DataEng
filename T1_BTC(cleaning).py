import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, col, regexp_replace, round
from pyspark.sql.window import Window



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


#file input e output
GT_input_path = "s3://progetto-professionai-aws/Raw Data/google_trend_bitcoin.csv"
BTC_input_path= "s3://progetto-professionai-aws/Raw Data/BTC_EUR.csv"

BTC_output_path="s3://crypto-silver-bucket/BTC.parquet"
GT_output_path = "s3://crypto-silver-bucket/GT_bitcoin.parquet"

# Funzione trasformazione da csv S3 a DynamicFrame Glue
def Csv_to_df(input_file):
    dynamic_frame= glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_file]},
    format="csv",
    format_options={"withHeader":True})
    return dynamic_frame
    
BTC_dynamicframe = Csv_to_df(BTC_input_path)
GT_dynamicframe = Csv_to_df(GT_input_path)
    

# DataFrame Spark sui dynamic_frame   
BTC_df = BTC_dynamicframe.toDF()
GT_df = GT_dynamicframe.toDF()

BTC_df.show(20)
print(BTC_df.columns)
# ['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %'] output CloudWatch Logs


# Drop colonne non necessarie
drop_columns= ['Open', 'High', 'Low', 'Vol.', 'Change %']
BTC_df = BTC_df.drop(*drop_columns)

# Rimuove le virgole nella colonna "Price"
BTC_df = BTC_df.withColumn("Price", regexp_replace(col("Price"), ",", ""))

# Casting colonne (la data ha formato diverso)
BTC_df = BTC_df.withColumn("Date", to_date(BTC_df["Date"], "MM/dd/yyyy")) \
        .withColumn("Price", BTC_df["Price"].cast("float"))

        
GT_df = GT_df.withColumn("Settimana", GT_df["Settimana"].cast("date")) \
        .withColumn("interesse bitcoin", GT_df["interesse bitcoin"].cast("float"))
        

        
        
# Pulizia valori mancanti per BTC ( sia per -1 che nulli in via generale)
window_spec = Window.orderBy("Date")

BTC_df = BTC_df.withColumn(
    "Price",
    F.when(
        (F.col("Price") == -1) | (F.col("Price").isNull()),  # condizioni
        (F.lag("Price").over(window_spec) + F.lead("Price").over(window_spec)) / 2  # Sostituisce con media tra gg precedente e successivo
    ).otherwise(F.col("Price"))
)

# Round sul prezzo
BTC_df = BTC_df.withColumn("Price", round(col("Price"), 3))

BTC_df.show(20)

   
        
#Salvataggio con Glue in formato parquet
def to_parquet(dataframe, output_path):
    
     transformed_dynamicframe = DynamicFrame.fromDF(dataframe, glueContext, "transformed_dynamicframe") 
 
     glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamicframe,
        connection_type="s3",
        connection_options={"path": output_path},
        format= "parquet")

count= BTC_df.groupBy("Date").agg( F.count("Date").alias("count"))
count.show(50)
        
to_parquet(BTC_df, BTC_output_path)
to_parquet(GT_df, GT_output_path)

