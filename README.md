# Spark-Structured-Streaming-w-Kafka
Analyzing San Francisco's Crime Rate using Kafka and Spark Structured Streaming

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Tried various values for maxOffsetsPerTrigger, startingOffsets, backpressure.enabled, spark.driver.memory, spark.executor.memory and couple others. Additionally, experimented with watermarking, window, and trigger interval values for queries. With the given hardware in Workspace, variety of configs helped improve the throughput numbers. However, latency wasn't much changed as parallelism wasn't possible with just one core in Workspace.

## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
Based on the improved throughput numbers, I believe maxOffsetsPerTrigger, startingOffsets, spark.driver.memory, and spark.executor.momory were most efficient properties of all.
