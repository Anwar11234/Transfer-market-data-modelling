from hdfs import InsecureClient

client = InsecureClient('http://namenode:9870/', user='root')

csv_directory = 'data/transformed/csv'
parquet_base_directory = 'data/transformed/parquet'
avro_base_directory = 'data/transformed/avro'

hdfs_csv_directory = '/user/data/csv'
hdfs_parquet_base_directory = '/user/data/parquet'
hdfs_avro_base_directory = '/user/data/avro'

total_csv_write_time = 0
total_parquet_write_times = {'snappy':0, 'gzip':0, 'brotli':0}
total_avro_write_times = {'snappy':0, 'deflate':0, 'zstandard':0}

parquet_compression_algorithms = ['snappy', 'gzip', 'brotli']
avro_compression_algorithms = ['snappy', 'deflate', 'zstandard']