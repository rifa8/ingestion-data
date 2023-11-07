import pandas as pd

pd.set_option('display.max_columns', None)

def read_csv():
    df = pd.read_csv('../dataset/renamed_yellow_tripdata_2020-07.csv', sep=',', low_memory=False)
    return df

def casting_data_type(df):
    df.dropna(inplace=True)
    df['vendor_id'] = df['vendor_id'].astype('int8')
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['pu_location_id'] = df['pu_location_id'].astype('int8')
    df['do_location_id'] = df['do_location_id'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')

    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    return df

df = read_csv()
print("BEFORE CAST:")
print(df.dtypes)
print('=======================================')

casted_df = casting_data_type(df)
print("AFTER CAST:")
print(casted_df.dtypes)
