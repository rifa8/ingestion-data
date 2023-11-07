import pandas as pd

pd.set_option('display.max_columns', None)

df = pd.read_csv('../dataset/yellow_tripdata_2020-07.csv', sep=',', low_memory=False)

df = df.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id', 'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'})

#untuk save file yang sudah di rename
df.to_csv('../dataset/renamed_yellow_tripdata_2020-07.csv', index=False)

print(df.dtypes)
