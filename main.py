# import all the necessary modules
import ingestion
import transformation
import fault_tolerance
import update_data
import storage_parallel

def main():
    # Data ingestion
    try:
        print("Data ingestion process started...")
        ingestion.main() # call the necessary functions
        print("Data ingestion process completed.")
    except Exception as e:
        print("Error occurred in data ingestion process:", e)
        return

    # Updating data
    try:
        print("Incremental processing started...")
        update_data.main() # call the necessary functions
        print("Incremental processing completed.")
    except Exception as e:
        print("Error occurred in incremental processing:", e)
        return

    # Data transformation
    try:
        print("Data transformation process started...")
        transformation.main() # call the necessary functions
        print("Data transformation process completed.")
    except Exception as e:
        print("Error occurred in data transformation process:", e)
        return
      
    # Data storage
    try:
        print("Storage parallel processing started...")
        storage_parallel.main() # call the necessary functions
        print("Storage parallel processing completed.")
    except Exception as e:
        print("Error occurred in storage parallel processing:", e)
        return

    # Handle any fault or error
    try:
        print("Fault tolerance process started...")
        fault_tolerance.main() # call the necessary functions
        print("Fault tolerance process completed.")
    except Exception as e:
        print("Error occurred in fault tolerance process:", e)
        return



if __name__ == "__main__":
    main()