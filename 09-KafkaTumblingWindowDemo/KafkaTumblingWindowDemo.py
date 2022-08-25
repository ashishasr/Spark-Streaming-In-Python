from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr, window
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Tumbling Window Demo") \
        .master("local[2]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    logger = Log4j(spark)

    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", IntegerType()),
        StructField("BrokerCode", StringType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stock-trades") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), stock_schema).alias("value"))

    trade_df = value_df.select("value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
        .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

    trade_df.printSchema()
    window_agg_df = trade_df \
        .groupBy(  # col("BrokerCode"),
        window(col("CreatedTime"), "15 minute")) \
        .agg(_sum("Buy").alias("TotalBuy"),
             _sum("Sell").alias("TotalSell"))

    output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")

    '''
    running_total_window = Window.orderBy("end") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    final_output_df = output_df \
        .withColumn("RTotalBuy", sum("TotalBuy").over(running_total_window)) \
        .withColumn("RTotalSell", sum("TotalSell").over(running_total_window)) \
        .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))

    final_output_df.show(truncate=False)
    '''
    window_query = output_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Waiting for Query")
    window_query.awaitTermination()
