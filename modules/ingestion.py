import pandas as pd
import json
import os

# Define base schemas for each type of data
BASE_CUSTOMER_SCHEMA = {
    "Customer_ID": "",
    "Last_Used_Platform": "",
    "Is_Blocked": "",
    "Created_At": "",
    "Language": "",
    "Outstanding_Amount": "",
    "Loyalty_Points": "",
    "Number_of_employees": ""
}

BASE_ORDERS_SCHEMA = {
    "Order_ID": "",
    "Order_Status": "",
    "Category_Name": "",
    # Add other fields as per your requirements
}

BASE_DELIVERIES_SCHEMA = {
    "Task_ID": "",
    "Order_ID": "",
    "Task_Status": "",
    # Add other fields as per your requirements
}


def load_csv(file_path):
    # Read CSV file and convert to a list of dictionaries
    data = pd.read_csv(file_path)
    data_list = data.to_dict(orient='records')
    return data_list


def load_json(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)


def handle_schema_evolution(data, base_schema):
    # Check for schema evolution
    new_fields = set(data.keys()) - set(base_schema.keys())
    removed_fields = set(base_schema.keys()) - set(data.keys())

    if new_fields:
        print("New fields detected:", new_fields)
        # Update base schema with new fields
        base_schema.update({field: "" for field in new_fields})

    if removed_fields:
        print("Removed fields detected:", removed_fields, '\n')
        # Handle removal of fields (optional, depending on requirements)

    return base_schema


def ingest_data(file_path, base_schema):
    file_extension = os.path.splitext(file_path)[1]
    if file_extension == '.csv':
        data = load_csv(file_path)
    elif file_extension == '.json':
        data = load_json(file_path)
    else:
        raise ValueError("Unsupported file format")

    # Handle schema evolution
    updated_schema = handle_schema_evolution(data[0], base_schema)

    # Process the data further as needed

    return data, updated_schema


def main():
    # Example usage
    customer_data, updated_customer_schema = ingest_data(
        'e-commerce-data-pipeline\Data\Market 1 Customers.json', BASE_CUSTOMER_SCHEMA)
    orders_data, updated_orders_schema = ingest_data(
        'e-commerce-data-pipeline\Data\Market 1 Orders.csv', BASE_ORDERS_SCHEMA)
    deliveries_data, updated_deliveries_schema = ingest_data(
        'e-commerce-data-pipeline\Data\Market 1 Deliveries.csv', BASE_DELIVERIES_SCHEMA)

    # Process the ingested data

    # Example: Print updated schemas
    print('\nUPDATES\n')
    print("Updated Customer Schema:", updated_customer_schema, '\n')
    print("Updated Orders Schema:", updated_orders_schema, '\n')
    print("Updated Deliveries Schema:", updated_deliveries_schema)


if __name__ == "__main__":
    main()