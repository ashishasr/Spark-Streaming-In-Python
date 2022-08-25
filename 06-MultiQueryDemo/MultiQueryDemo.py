from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("KafkaStreamDemo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    # kafka_df.printSchema()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    value_df.printSchema()
    # value_df.show()
    """ Creating a Notification record written to kafka sink"""
    notification_rec_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
        .withColumn("EarnedLoyalityPoints", expr("TotalAmount * 0.2"))
    notification_rec_df.printSchema()

    kafka_target_df = notification_rec_df.selectExpr("InvoiceNumber as key",
                                                     """ to_json(named_struct(
                                                    'CustomerCardNo' , CustomerCardNo,
                                                    'TotalAmount', TotalAmount,
                                                    'EarnedLoyalityPoints', TotalAmount * 0.2)) as value """)

    notification_writer_query = kafka_target_df.writeStream \
        .format("kafka") \
        .queryName("Notification invoice Writer") \
        .outputMode("append") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "notifications") \
        .option("checkpointLocation", "chk-point-dir/notify") \
        .start()

    logger.info("Listening and Writing to Kafka")

    """ Creating a invoice record written to file sink"""
    explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
                                     "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType",
                                     "value.DeliveryAddress.City",
                                     "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode",
                                     "explode(value.InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir/flatten") \
        .start()

    logger.info("Listening to Kafka")

    spark.streams.awaitAnyTermination()