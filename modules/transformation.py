
import pandas as pd
import json
import os


def load_csv(file_path):
    return pd.read_csv(file_path)


def load_json(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        return pd.DataFrame.from_records(data)


def incremental_update(file_path, last_processed_id, id_column):
    data = load_csv(file_path)
    if id_column in data.columns:
        new_or_modified_data = data[data[id_column] > last_processed_id]
        return new_or_modified_data[id_column].max() if not new_or_modified_data.empty else last_processed_id
    else:
        print(f"ID column '{
              id_column}' not found in the data. Unable to perform incremental update.")
        return last_processed_id


def data_transformation_and_aggregate_metrics(customers_data, orders_data, deliveries_data):
    # Merge orders and deliveries data with customer information
    orders_enriched = pd.merge(
        orders_data, customers_data, on='Order_ID', how='left')
    deliveries_enriched = pd.merge(
        deliveries_data, customers_data, on='Order_ID', how='left')

    # Calculate aggregate metrics for orders
    aggregate_metrics_orders = orders_enriched.groupby('Customer_ID').agg(
        total_transaction_amount=('Total_Price', 'sum'),
        average_order_value=('Total_Price', 'mean'),
        total_number_of_orders=('Order_ID', 'count')
    )

    # Calculate additional aggregate metrics for deliveries
    aggregate_metrics_deliveries = deliveries_enriched.groupby('Customer_ID').agg(
        total_revenue=('Total_Price', 'sum'),
        total_quantity=('Quantity', 'sum'),
        average_delivery_time=('Total_Time_Taken', 'mean')
    )

    # Merge aggregate metrics dataframes
    final_metrics = pd.merge(
        aggregate_metrics_orders, aggregate_metrics_deliveries, on='Customer_ID', how='outer')

    return final_metrics


def main():
    # Example usage
    last_processed_customer_id = 0  # Assume initial value
    last_processed_orders_id = 0    # Assume initial value
    last_processed_deliveries_id = 0  # Assume initial value

    # Incremental update for customers data
    last_processed_customer_id = incremental_update(
        'e-commerce-data-pipeline\Data\Market 1 Customers.json', last_processed_customer_id, id_column='Customer_ID')

    # Incremental update for orders data
    last_processed_orders_id = incremental_update(
        'e-commerce-data-pipeline\Data\Market 1 Orders.csv', last_processed_orders_id, id_column='Order_ID')

    # Incremental update for deliveries data
    last_processed_deliveries_id = incremental_update(
        'e-commerce-data-pipeline\Data\Market 1 Deliveries.csv', last_processed_deliveries_id, id_column='Delivery_ID')

    # Load data
    customers_data = load_json(
        'e-commerce-data-pipeline\Data\Market 1 Customers.json')
    orders_data = load_csv('e-commerce-data-pipeline\Data\Market 1 Orders.csv')
    deliveries_data = load_csv(
        'e-commerce-data-pipeline\Data\Market 1 Deliveries.csv')

    # Perform data transformation and aggregate metrics calculation
    final_metrics = data_transformation_and_aggregate_metrics(
        customers_data, orders_data, deliveries_data)

    # Print final metrics dataframe
    print("Final Metrics:\n")
    print(final_metrics)


if __name__ == "__main__":
    main()
