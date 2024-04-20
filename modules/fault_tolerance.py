
from pyspark.sql import SparkSession

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("FaultTolerance") \
        .getOrCreate()
    
    try:
        # Enable checkpointing
        spark.sparkContext.setCheckpointDir(
            "hdfs://<HDFS_MASTER>:<PORT>/checkpoint_dir")  # specify path 
    
        # Read data
        data_df = spark.read.csv(
            "hdfs://<HDFS_MASTER>:<PORT>/input_data.csv", header=True)  # specify path url
    
        # Perform transformations
        transformed_df = data_df.withColumn(
            "transformed_column", data_df["original_column"] * 2)
    
        # Checkpoint intermediate DataFrame
        transformed_df.checkpoint()
    
        # Perform further transformations
        processed_df = transformed_df.filter(
            transformed_df["transformed_column"] > 100)
    
        # Configure task retry
        # Maximum number of task failures allowed
        spark.conf.set("spark.task.maxFailures", 4)
    
        # Execute an action to trigger job execution
        result = processed_df.count()
    
    except Exception as e:
        # Handle exceptions gracefully
        print("Error occurred:", str(e))
        # Perform necessary cleanup or recovery actions
    
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == '__main__':
    main()
