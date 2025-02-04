# TransferMarket Data Modeling

## Project Overview
This project models the [Transfermarket dataset](https://www.kaggle.com/datasets/davidcariboo/player-scores) as a star schema and then implements the model on HDFS in CSV, Avro and Parquet formats using different compression algorithms and compares between formats in terms of size, write speed and read speed.

## Data Modelling
The first phase of the project focuses on data modeling. The dataset is modeled around two key business processes:

1. **Player Appearances**: Tracks player appearances in games, with dimensions including player, game, competition, and club.  
   ![Player Appearances Data Model](data%20model/appearances.png)

2. **Player Transfers**: Captures player transfers between clubs, with dimensions including player and club.  
   ![Player Transfers Data Model](data%20model/transfers.png)

Once the data model was designed, Python was used to perform the necessary transformations to align the data with the star schema. The final dimension and fact tables were stored as CSV files.

---

## Converting to Parquet and Avro

- **Parquet Conversion**: The CSV files were converted to Parquet format using the `pyarrow` Python library. The conversion was repeated multiple times, each time applying a different compression algorithm: `snappy`, `gzip`, and `brotli`. This allowed for a comprehensive comparison of the impact of compression on file size and performance.

- **Avro Conversion**: Similarly, the CSV files were converted to Avro format using the `fastavro` Python library. The conversion was performed with three compression algorithms: `snappy`, `deflate`, and `zstandard`. This step ensured a thorough evaluation of Avro's efficiency under different compression settings.

---

## Uploading to HDFS

- All files—CSV, Parquet, and Avro—were uploaded to an HDFS cluster. The cluster was set up using `docker-compose`, ensuring a consistent and reproducible environment for testing.

- The Python script interacted with the HDFS cluster using the `hdfs` library. By running the script as a Docker container, it operated on the same network as the HDFS cluster, enabling efficient and reliable communication between the script and the cluster.


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
