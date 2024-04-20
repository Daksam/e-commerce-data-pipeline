
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count


def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ECommerceDataPipeline") \
        .getOrCreate()
    
    try:
        # Data Ingestion with Spark
        # modify the file paths based on your file paths
        customers_df = spark.read.json("customers.json")
        orders_df = spark.read.csv("orders.csv", header=True)
        deliveries_df = spark.read.csv("deliveries.csv", header=True)
    
        # Data Transformation with Spark
        joined_df = orders_df.join(deliveries_df, "Order_ID", "inner")
    
        # Aggregate Metrics Calculation with Spark
        # modify this based on your column names
        customer_metrics_df = joined_df.groupBy("Customer_ID").agg(
            sum("Total_Price").alias("Total_Transaction_Amount"),
            avg("Total_Price").alias("Average_Order_Value"),
            count("Order_ID").alias("Total_Number_of_Orders")
        )
    
        # Scalability Considerations
        # Data Storage
        # Choose a suitable storage solution such as HDFS or a distributed database to store the processed data
        customer_metrics_df.write.mode("overwrite").parquet(
            "hdfs://<HDFS_MASTER>:<PORT>/e_commerce_data")  # specify your path url or path on your local pc
    
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == '__main__':
    main()
