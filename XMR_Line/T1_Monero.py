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


#file input
GT_input_path = "s3://progetto-professionai-aws/Raw Data/google_trend_monero.csv"
XMR_input_path= "s3://progetto-professionai-aws/Raw Data/XMR_EUR.csv"

XMR_output_path="s3://crypto-silver-bucket/XMR.parquet"
GT_output_path = "s3://crypto-silver-bucket/GT_monero.parquet"

# Funzione trasformazione da csv S3 a DynamicFrame Glue
def Csv_to_df(input_file):
    dynamic_frame= glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_file]},
    format="csv",
    format_options={"withHeader":True})
    return dynamic_frame
    
XMR_dynamicframe = Csv_to_df(XMR_input_path)
GT_dynamicframe = Csv_to_df(GT_input_path)
    

# DataFrame Spark sui dynamic_frame   
XMR_df = XMR_dynamicframe.toDF()
GT_df = GT_dynamicframe.toDF()

XMR_df.show(20)
print(XMR_df.columns)
# ['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %'] output CloudWatch Logs



# Drop colonne non necessarie solo per XMR
drop_columns= ['Open', 'High', 'Low', 'Vol.', 'Change %']
XMR_df = XMR_df.drop(*drop_columns)

# Rimuovere le virgole dai valori nella colonna "Price"
XMR_df = XMR_df.withColumn("Price", regexp_replace(col("Price"), ",", ""))

# Casting colonne (la data ha formato diverso)
XMR_df = XMR_df.withColumn("Date", to_date(XMR_df["Date"], "MM/dd/yyyy")) \
        .withColumn("Price", XMR_df["Price"].cast("float"))

        
GT_df = GT_df.withColumn("Settimana", GT_df["Settimana"].cast("date")) \
        .withColumn("Monero_interesse", GT_df["Monero_interesse"].cast("float"))
        
        
        
# Pulizia valori mancanti per BTC ( sia -1 che nulli in via generale)
window_spec = Window.orderBy("Date")

XMR_df = XMR_df.withColumn(
    "Price",
    F.when(
        (F.col("Price") == -1) | (F.col("Price").isNull()),  # condizioni
        (F.lag("Price").over(window_spec) + F.lead("Price").over(window_spec)) / 2  # Sostituisce con media tra gg precedente e successivo
    ).otherwise(F.col("Price"))
)

# Round sul prezzo
XMR_df = XMR_df.withColumn("Price", round(col("Price"), 3))



        
#Salvataggio con Glue in formato parquet
def to_parquet(dataframe, output_path):
    
     transformed_dynamicframe = DynamicFrame.fromDF(dataframe, glueContext, "transformed_dynamicframe") 
 
     glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamicframe,
        connection_type="s3",
        connection_options={"path": output_path},
        format= "parquet")
        
        
XMR_df.show(20)
GT_df.show(20)


to_parquet(XMR_df, XMR_output_path)
to_parquet(GT_df, GT_output_path)

