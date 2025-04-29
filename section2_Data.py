from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round as spark_round, to_date, hour

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Hourly Aggregation Sorted and Rounded") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Load your combined data
df = spark.read.option("header", True).option("inferSchema", True).csv("output/combined_data_singlefile/")

# Step 3: Add date and hour columns
date_df = df.withColumn("date", to_date("timestamp")) \
            .withColumn("hour", hour("timestamp"))

# Step 4: Group by date, hour, region and aggregate
hourly_agg = date_df.groupBy("date", "hour", "region").agg(
    avg("pm25").alias("avg_pm25"),
    avg("temperature").alias("avg_temp"),
    avg("humidity").alias("avg_humidity")
)

# Step 5: Round to 2 decimal places
hourly_agg_rounded = hourly_agg.select(
    col("date"),
    col("hour"),
    col("region"),
    spark_round(col("avg_pm25"), 2).alias("avg_pm25"),
    spark_round(col("avg_temp"), 2).alias("avg_temp"),
    spark_round(col("avg_humidity"), 2).alias("avg_humidity")
)

# Step 6: Sort by date and hour ascending
hourly_agg_sorted = hourly_agg_rounded.orderBy(col("date").asc(), col("hour").asc())

# Step 7: Save the final sorted, rounded output
hourly_agg_sorted.write.mode("overwrite").option("header", True).csv("output/section2_transformed/hourly_aggregates_sorted")

print("✅ Successfully grouped, rounded to 2 decimals, sorted by date and hour.")
