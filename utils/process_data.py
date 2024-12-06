from pyspark.sql import SparkSession
import time

def main():
    spark = SparkSession.builder \
        .appName("Demo_DE") \
        .master("spark://node1:7077") \
        .getOrCreate()

    # dataset = "/mnt/mycephfs/demo/parquet_without_partition/TEMP"
    # dataset =  "/mnt/mycephfs/demo/parquet_without_partition/HHOT"
    # df = spark.read.parquet(dataset)
    # df.write.mode("overwrite").partitionBy("station").parquet(dataset.replace("without_partition","with_partition"))

    # dataset, file_type = "/mnt/mycephfs/demo/parquet_with_partition/HHOT", 'parquet'
    # dataset, file_type = "/mnt/mycephfs/demo/parquet_without_partition/HHOT", 'parquet'
    dataset, file_type = "/mnt/mycephfs/demo/format_json_without_partition/HHOT", 'json'
    # query_str = "SELECT COUNT(*) FROM data WHERE MM='03'"
    query_str = "SELECT COUNT(*) FROM data WHERE station='CCH' AND MM='03'"

    # dataset, file_type = "/mnt/mycephfs/demo/parquet_with_partition/TEMP", 'parquet'
    # dataset, file_type = "/mnt/mycephfs/demo/parquet_without_partition/TEMP", 'parquet'
    # dataset, file_type = "/mnt/mycephfs/demo/format_json_without_partition/TEMP", 'json'
    # query_str = "SELECT COUNT(*) FROM data WHERE data_type='CLMMINT'"
    # query_str = "SELECT COUNT(*) FROM data WHERE station='CCH' AND data_type='CLMMINT'"

    print(f"Dataset: {dataset}")
    print(f"Query string: {query_str}")
    for _ in range(6):
        start_time = time.perf_counter_ns()
        if file_type == 'parquet':
            df = spark.read.parquet(dataset)
        elif file_type == 'json':
            df = spark.read.json(dataset)
        df.createOrReplaceTempView("data")
        time_load = time.perf_counter_ns() - start_time

        start_time = time.perf_counter_ns()
        result = spark.sql(query_str)
        result.show()
        time_query = time.perf_counter_ns() - start_time

        print(f"Load time: {time_load/10**9:.4f} s, Query time: {time_query/10**9:.4f} s")
    
    result = spark.sql("SELECT COUNT(*) FROM data")
    result.show()

    spark.stop()

if __name__ == "__main__":
    main()
