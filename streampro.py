from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("EmojiAggregation").getOrCreate()

# Define schema for incoming JSON data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read data from Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "emoji_topic").option("failOnDataLoss", "false").load()

# Convert the Kafka "value" from binary to string, then parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as json_data").select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# Perform windowed aggregation with scaling logic
agg_df = parsed_df \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(
        window(col("timestamp"), "2 seconds"),  # Group into 2-second intervals
        col("emoji_type")
    ) \
    .count() \
    .withColumn("scaled_count", expr("ceil(count / 1000)"))  # Scale down count
    
result_df = agg_df.selectExpr(
    "CAST(emoji_type AS STRING) AS key",  # Use emoji_type as key
    "CAST(scaled_count AS STRING) AS value",  # Use scaled_count as value (cast to appropriate type, e.g., DOUBLE)
)

query = result_df \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output_topic") \
    .option("checkpointLocation", "spark_checkpoint") \
    .option("failOnDataLoss", "false")\
    .outputMode("append") \
    .start()
query.awaitTermination()
