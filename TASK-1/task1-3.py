import pandas as pd

pd.set_option('display.max_columns', None)

df = pd.read_csv('../dataset/renamed_yellow_tripdata_2020-07.csv', sep=',', low_memory=False)

multi_cols_desc = df.sort_values(by='passenger_count', ascending=False)[['vendor_id', 'passenger_count', 'trip_distance', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']]

print(multi_cols_desc.head(10))
