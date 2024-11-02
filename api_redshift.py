import redshift_connector
import pandas as pd
import matplotlib.pyplot as plt 
import seaborn as sns
    
conn_info = { 
    "host":'cypto-spacenames.626635429853.eu-north-1.redshift-serverless.amazonaws.com',
    "database":'dev',
    "user":'admin',
    "password":'JW28krhps',
    "port":5439
}



query = "SELECT * FROM public.bitcoin;"  

with redshift_connector.connect(**conn_info) as conn:
    with conn.cursor() as cursor :
        
        cursor.execute(query)
        
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        # Crea il DataFrame Pandas
        df = pd.DataFrame(result, columns=column_names)



# Normalizzazione delle colonne
df['ma10_norm'] = (df['ma10'] - df['ma10'].min()) / (df['ma10'].max() - df['ma10'].min())
df['google_trend_norm'] = (df['interesse_bitcoin'] - df['interesse_bitcoin'].min()) / (df['interesse_bitcoin'].max() - df['interesse_bitcoin'].min())

sns.set_theme(style="whitegrid")
plt.figure(figsize=(12, 6))
# Traccia le colonne normalizzate
sns.lineplot(x=df['settimana'], y= df['ma10_norm'], color='blue', label='BTC ma10 Normalized')
sns.lineplot(x=df['settimana'], y=df['google_trend_norm'], color='orange', label='Google Trend Normalized')


plt.show()
