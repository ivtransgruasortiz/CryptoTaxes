import pandas as pd
import tabula
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# SPARKSESSION
spark = SparkSession\
    .builder\
    .appName("CryptoTaxes")\
    .getOrCreate()

# PANDAS VISUALIZATION OPTIONS
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
# LOAD DATAFRAME OPERATIONS
df = tabula.read_pdf('./in/mov_coinbase_prueba.pdf', pages='all')[2:].iloc[:, 1:]
columns = df.iloc[0].tolist()
df = df.iloc[1:]
df.columns = columns
df.reset_index(drop=True, inplace=True)
df_1 = df.copy()
df_1['Date'] = pd.to_datetime(df_1['Date'])
df_1[['Qty.', 'Price']] = df_1[['Qty.', 'Price']].astype('float')
df_1['Fee'] = df_1['Fee'].str.split(expand=True)[0].astype('float')
df_1['Total'] = df_1['Total'].replace(' ', '', regex=True).str.split('EUR', expand=True)[0].astype('float')
df_1 = df_1.sort_values(by='Date')
valor_residual_cartera = 6200  # eur en cartera coinbase en momento calculo 31 diciembre por ejemplo
valor_beneficios_declaracion = df_1['Total'].sum() + valor_residual_cartera
df_1.to_excel('./mov.xlsx')
# df1 = [x.columns[1:] for x in df]
# df1_df = pd.DataFrame(df1)
# df2_df = df1_df.sort_values(0)
# df.columns

# SPARK TREATMENT FOR AGGREGATE
dfs_1 = spark.createDataFrame(df_1)
dfs_1.show(4, False)
dfs_1.printSchema()
dfs_2 = dfs_1.groupby('Product').agg({'Total': 'sum'})\
    .withColumnRenamed("sum(Total)", "sum_oper")
dfs_2.agg({'sum_oper': 'sum'}).show()
## OPTIONAL
dfs_3 = dfs_2.filter(f.col('Product') != 'LTC-EUR')
dfs_3.agg({'sum_oper': 'sum'}).show()
