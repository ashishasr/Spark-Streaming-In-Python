from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("File Streaming Source Demo")\
        .master("local[3]")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.sql.streaming.schemaInference", "true")\
        .getOrCreate()

    logger = Log4j(spark)
    raw_df = spark.readStream\
        .format("json")\
        .option("path", "input")\
        .option("maxFilesPerTrigger", "1")\
        .option("cleanSource", "delete")\
        .load()

    #raw_df.printSchema()

    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "CustomerType", "CustomerCardNo",
                                   "PaymentMethod", "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State",
                                   "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")
    #explode_df.printSchema()

    flattened_df = explode_df\
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    flattened_df.printSchema()

    invoice_writer_query = flattened_df.writeStream\
        .format("json")\
        .option("path", "output")\
        .option("checkpointLocation", "chk-point-dir")\
        .outputMode("append")\
        .trigger(processingTime="1 minute")\
        .queryName("Flattened invoice Writer ")\
        .start()

    logger.info("Flattened invoice Writer Started")
    invoice_writer_query.awaitTermination()


