import pandas as pd
from tabula import read_pdf


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


df = read_pdf('./in/mov_coinbase_prueba.pdf', pages='all')[2:].iloc[:, 1:]
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