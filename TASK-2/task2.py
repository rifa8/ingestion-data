import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, DateTime, Boolean, Float, Integer

pd.set_option('display.max_columns', None)

# 1 & 2) Create a DataFrame from a Parquet file and Load the parquet file to a DataFrame with fastparquet
def read_parquet():
    df = pd.read_parquet('../dataset/yellow_tripdata_2023-01.parquet', engine='fastparquet')
    return df

# 3) Clean the Yellow Trip dataset
def cleaning(df):
    df.dropna(inplace=True)

    #cast data type
    df['VendorID'] = df['VendorID'].astype('int8')
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['PULocationID'] = df['PULocationID'].astype('int8')
    df['DOLocationID'] = df['DOLocationID'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')

    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    #Rename columns with snake_case format
    df = df.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id', 'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'})
    
    return df

def connect_db():
    user = 'postgres'
    password = 'admin'
    host = '192.168.0.12'
    database = 'mydb'
    port = 5432
    
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string)

    return engine

# 4) Define the data type schema
def to_sql(df, engine):
    df_schema = {
        'vendor_id': BigInteger,
        'tpep_pickup_datetime': DateTime,
        'tpep_dropoff_datetime': DateTime,
        'passenger_count': BigInteger,
        'trip_distance': Float,
        'rate_code_id': Float,
        'store_and_fwd_flag': Boolean,
        'pu_location_id': BigInteger,
        'do_location_id': BigInteger,
        'payment_type': BigInteger,
        'fare_amount': Float,
        'extra': Float,
        'mta_tax': Float,
        'tip_amount': Float,
        'tolls_amount': Float,
        'improvement_surcharge': Float,
        'total_amount': Float,
        'congestion_surcharge': Float,
        'airport_fee': Float
    }

    df.to_sql(name='parquet', con=engine, if_exists="replace", index=False, schema="public", dtype=df_schema, method=None, chunksize=5000)

# 5) Ingest the Yellow Trip dataset to PostgreSQL
if __name__ == "__main__":
    df = read_parquet()
    cleaned_df = cleaning(df)
    conn = connect_db()
    to_sql(cleaned_df, conn)
