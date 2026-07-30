import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def build_spark_lakehouse_session() -> SparkSession:
    """
    Initializes a distributed Spark Session pre-configured with 
    Scala 2.13 compatible production-grade jar packages for Kafka and S3.
    """
    return SparkSession.builder \
        .appName("LakehouseStreamingProcessor") \
      .config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3,"
    "org.apache.hadoop:hadoop-aws:3.4.1"
)\
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin_key") \
        .config("spark.hadoop.fs.s3a.secret.key", "secret_session_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000") \
        .getOrCreate()

def run_streaming_pipeline():
    spark = build_spark_lakehouse_session()
    print("Loaded Jars")
    print(
    spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
)
    spark.sparkContext.setLogLevel("WARN")
    
    # ─── FORCE INJECTION INTO THE BACKGROUND FILESYSTEM THREADS ───
    # This explicitly overrides the internal Hadoop defaults with pure numeric integers
    sc = spark.sparkContext
    hc = sc._jsc.hadoopConfiguration()
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    hc.set("fs.s3a.connection.timeout", "60000")
    hc.set("fs.s3a.connection.establish.timeout", "60000")
    hc.set("fs.s3a.connection.acquisition.timeout", "60000") # ◄── CRITICAL KNOCKOUT BLOW FOR "60s"
    
    print("\n⚡ PYSPARK STRUCTURED STREAMING ENGINE INITIALIZED ⚡\n")

    # 1. Read the live byte stream from the local Kafka Broker Topic
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "live_market_telemetry") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Define the Explicit Schema to parse the raw unstructured JSON
    crypto_schema = StructType([
        StructField("ingested_at", LongType(), True),
        StructField("metrics", StructType([
            StructField("bitcoin", StructType([StructField("usd", DoubleType(), True), StructField("usd_24h_vol", DoubleType(), True)]), True),
            StructField("ethereum", StructType([StructField("usd", DoubleType(), True), StructField("usd_24h_vol", DoubleType(), True)]), True),
            StructField("solana", StructType([StructField("usd", DoubleType(), True), StructField("solana_24h_vol", DoubleType(), True)]), True)
        ]), True)
    ])

    # 3. Transform the byte array data
    parsed_df = raw_kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), crypto_schema).alias("data")) \
        .select(
            col("data.ingested_at").alias("extraction_timestamp"),
            col("data.metrics.bitcoin.usd").alias("bitcoin_price"),
            col("data.metrics.ethereum.usd").alias("ethereum_price"),
            col("data.metrics.solana.usd").alias("solana_price")
        )

    # 4. Stream write the processed columnar data out into MinIO S3 bucket as Parquet formats
    query = parsed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://telemetry-data-lake/bronze/market_metrics") \
        .option("checkpointLocation", "s3a://telemetry-data-lake/checkpoints/market_metrics") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("🛰️  CRITICAL STREAM ACTIVATED: Streaming data down to MinIO every 10 seconds...")
    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_pipeline()