import os
import pandas as pd
import config
import time
from data_extraction import load_data
import data_transformation
import conversions

df_games, df_clubs, df_player, df_competitions, fact_appearance, df_fact_transfer = load_data()

df_games, df_clubs, df_player, df_competitions = data_transformation.add_surrogate_keys(df_games, df_clubs, df_player, df_competitions)

df_games = data_transformation.transform_games(df_games, df_competitions)

df_clubs = data_transformation.transform_clubs(df_clubs, df_competitions)

df_player = data_transformation.transform_players(df_player)

df_competitions = data_transformation.transform_competitions(df_competitions)

fact_appearance = data_transformation.transform_fact_appearance(fact_appearance, df_games, df_clubs, df_competitions, df_player)

df_fact_transfer = data_transformation.transform_fact_transfer(df_fact_transfer, df_player, df_clubs)

data_transformation.write_data(df_games, df_clubs, df_player, df_competitions, fact_appearance, df_fact_transfer)

csv_files = [f for f in os.listdir(config.csv_directory) if f.endswith('.csv')]

for csv_file in csv_files:
  file_path = os.path.join(config.csv_directory, csv_file)
  df = pd.read_csv(file_path)
  
  for algo in config.parquet_compression_algorithms:
    conversions.csv_to_parquet(df, csv_file,config.parquet_base_directory,algo)
    print(f'Converted {csv_file} to Parquet ({algo})')
  
  for algo in config.avro_compression_algorithms:
    conversions.csv_to_avro(df,csv_file,config.avro_base_directory,algo)
    print(f'Converted {csv_file} to AVRO ({algo})')

for csv_file in csv_files:
  file_path = os.path.join(config.csv_directory, csv_file)
  hdfs_csv_path = os.path.join(config.hdfs_csv_directory, csv_file)
  
  start = time.time()
  with config.client.write(hdfs_csv_path) as writer:
      pd.read_csv(file_path).to_csv(writer, index=False)
  end = time.time()
  elapsed_time = end - start
  config.total_csv_write_time += elapsed_time
  print(f'Uploaded {csv_file} to {hdfs_csv_path} in {elapsed_time:.2f} seconds')

for algo in config.parquet_compression_algorithms:
  local_parquet_dir = os.path.join(config.parquet_base_directory, algo)
  hdfs_parquet_dir = os.path.join(config.hdfs_parquet_base_directory, algo)
  
  for parquet_file in os.listdir(local_parquet_dir):
    local_parquet_path = os.path.join(local_parquet_dir, parquet_file)
    hdfs_parquet_path = os.path.join(hdfs_parquet_dir, parquet_file)
    
    start = time.time()
    with open(local_parquet_path, 'rb') as f:
      config.client.write(hdfs_parquet_path, f, overwrite=True)
    end = time.time()
    elapsed_time = end - start
    config.total_parquet_write_times[algo] += elapsed_time
    print(f'Uploaded {parquet_file} ({algo}) to {hdfs_parquet_path} in {elapsed_time:.2f} seconds')

for algo in config.avro_compression_algorithms:
  local_avro_dir = os.path.join(config.avro_base_directory, algo)
  hdfs_avro_dir = os.path.join(config.hdfs_avro_base_directory, algo)
  
  for avro_file in os.listdir(local_avro_dir):
    local_avro_path = os.path.join(local_avro_dir, avro_file)
    hdfs_avro_path = os.path.join(hdfs_avro_dir, avro_file)
    
    start = time.time()
    with open(local_avro_path, 'rb') as f:
      config.client.write(hdfs_avro_path, f, overwrite=True)
    end = time.time()
    elapsed_time = end - start
    config.total_avro_write_times[algo] += elapsed_time
    print(f'Uploaded {avro_file} ({algo}) to {hdfs_avro_path} in {elapsed_time:.2f} seconds')

print(f'Total time to upload all CSV files: {config.total_csv_write_time:.2f} seconds')
print(f'Total time to upload all Parquet files: {config.total_parquet_write_times}')
print(f'Total time to upload all Avro files: {config.total_avro_write_times}')

directories = [
  "/user/data/csv",
  "/user/data/parquet/snappy",
  "/user/data/parquet/gzip",
  "/user/data/parquet/brotli",
  "/user/data/avro/snappy",
  "/user/data/avro/zstandard",
  "/user/data/avro/deflate",
]

# Store results
results = []

# Function to measure read time
def measure_read_time(file_path):
  start_time = time.perf_counter()
  with config.client.read(file_path) as reader:
    data = reader.read()  # Read entire file
  end_time = time.perf_counter()
  return end_time - start_time

# Process each directory
for directory in directories:
  read_time = 0
  files = config.client.list(directory)
  for file in files:
    file_path = f"{directory}/{file}"
    read_time += measure_read_time(file_path)
  results.append({"Directory": directory, "Read Time (s)": read_time})

print(results)