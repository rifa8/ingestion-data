import pandas as pd

pd.set_option('display.max_columns', None)

df = pd.read_csv('../dataset/yellow_tripdata_2020-07.csv', sep=',', low_memory=False)

print(df.head(2))
