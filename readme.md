# TransferMarket Data Modeling

## Project Overview
This project models the [Transfermarket dataset](https://www.kaggle.com/datasets/davidcariboo/player-scores) as a star schema and then implements the model on HDFS in CSV, Avro and Parquet formats using different compression algorithms and compares between formats in terms of size, write speed and read speed.

## Data Modelling

The first part of this project is data modelling, the data models focuses on 2 main business processes: 
1. **player appearances** in games with dimensions player, game, competition and club. 
![alt text](<data model/appearances.png>)
2. **player transfers** from club to another with dimensions player and club. 
![alt text](<data model/transfers.png>)

After designing the data model, Python was used to perform various transformations to make the data follow the star schema. Then final dimension and fact tables are stored as CSV files.

## Converting to Parquet and AVRO
- The model's CSV files are converted to parquet files using `pyarrow` Python library, the conversion was done multiple times using different compression algorithms, specifically: `snappy`, `gzip`, `brotli`. 

- CSV files are also converted to AVRO using `fastavro` Python library, the conversion was also done multiple times using different compression algorithms, specifically: `snappy`, `deflate`, `zstandard`.

## Uploading to HDFS 
- All files were uploaded to an HDFS cluster with that was setup using `docker-compose`.
- Python's `hdfs` library was used to interact with the HDFS cluster through Python.

## Comparing sizes
This table compares sizes for various data formats stored in HDFS. The actual size represents the size of the data as stored in HDFS, excluding replication. **CSV** files are the largest because they are uncompressed. **Parquet** files are smaller than CSV 
while **Avro** files are smaller than CSV but larger than Parquet.

| **Data Format** | **Size** |
|-----------------|-----------------|
| CSV             | 121.6 M         |
| Parquet (Snappy)| 24.4 M          |
| Parquet (Gzip)  | 15.4 M          |
| Parquet (Brotli)| 13.3 M          |
| Avro (Snappy)   | 40.6 M          |
| Avro (Deflate)  | 27.6 M          |
| Avro (Zstandard)| 28.2 M          |

## Comparing Write Speeds
This table summarizes the total time taken to upload files in different formats and compression types.

| **File Format** | **Write Time (seconds)** |
|-----------------|---------------------------|
| CSV             | 77.87                     |
| Parquet (Snappy)| 0.8229                    |
| Parquet (Gzip)  | 0.8731                    |
| Parquet(Brotli) | 0.8437                    |
| Avro(Snappy)    | 0.8763                    |
| Avro(Deflate)   | 2.2978                    |
| Avro(Zstandard) | 3.4683                    |

- **CSV** files take significantly longer to upload compared to **Parquet** and **Avro** files due to their larger size and lack of compression.
- **Parquet** files have the fastest upload times.
- **Avro** files are slower to upload than Parquet, with the slowest upload time for the last entry.

## Comparing Read Speeds
This table summarizes the read times for files in different formats and compression types.

| **Format**               | **Read Time (seconds)** |
|--------------------------|-------------------------|
| CSV                      | 1.2119                  |
| Parquet (Snappy)         | 0.1994                  |
| Parquet (Gzip)           | 0.1552                  |
| Parquet (Brotli)         | 0.1119                  |
| Avro (Snappy)            | 0.2533                  |
| Avro (Zstandard)         | 0.1730                  |
| Avro (Deflate)           | 0.1920                  |

- **CSV** files have the slowest read time due to their lack of compression and row-based format.
- **Parquet** files are the fastest to read, with **Brotli** compression being the most efficient.
- **Avro** files are slower than Parquet but faster than CSV, with **Snappy** being the slowest among Avro compression types.