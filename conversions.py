import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from fastavro import writer, parse_schema
import os

def csv_to_parquet(df,csv_file,parquet_base_directory, algo):
    table = pa.Table.from_pandas(df)
    local_parquet_dir = os.path.join(parquet_base_directory, algo)
      
    parquet_filename = csv_file.replace('.csv', '.parquet')
    local_parquet_path = os.path.join(local_parquet_dir, parquet_filename)
    pq.write_table(table, local_parquet_path, compression=algo)

def csv_to_avro(df,csv_file,avro_base_directory, algo):
    local_avro_dir = os.path.join(avro_base_directory, algo)
      
    avro_filename = csv_file.replace('.csv', '.avro')
    local_avro_path = os.path.join(local_avro_dir, avro_filename)
    
    schema = generate_avro_schema(df)
    parsed_schema = parse_schema(schema)
    with open(local_avro_path, 'wb') as out_file:
        writer(out_file, parsed_schema, df.replace({pd.NA: None}).to_dict('records'), codec=algo)

def generate_avro_schema(df):
  schema = {
    "type": "record",
    "name": "GameRecord",
    "fields": []
  }
  for column in df.columns:
    dtype = df[column].dtype
    # Map pandas dtype to Avro type
    if dtype == "int64":
        avro_type = "long"
    elif dtype == "float64":
        avro_type = "double"
    elif dtype == "bool":
        avro_type = "boolean"
    else:
        avro_type = "string"  # Default to string for objects/datetimes
    
    # Handle nullable fields (if column has NaN values)
    if df[column].isnull().any():
        avro_type = ["null", avro_type]
    
    schema["fields"].append({
        "name": column,
        "type": avro_type
    })
  return schema