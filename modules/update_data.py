
import json
import pandas as pd
import os


def load_csv(file_path):
    return pd.read_csv(file_path)


def incremental_update(file_path, last_processed_user):
    data = load_csv(file_path)

    # Check if last processed user exists and find its index in the data
    if last_processed_user is not None:
        try:
            last_index = data.index[data.apply(lambda row: row.equals(
                last_processed_user), axis=1)].tolist()[0]
            new_or_modified_data = data.iloc[last_index+1:]
        except IndexError:
            new_or_modified_data = pd.DataFrame()
    else:
        new_or_modified_data = data

    last_processed_user = new_or_modified_data.iloc[-1] if not new_or_modified_data.empty else None
    return last_processed_user


def main():
    files = ['e-commerce-data-pipeline\Data\Market 1 Orders.csv',
             'e-commerce-data-pipeline\Data\Market 2 Orders.csv',
             'e-commerce-data-pipeline\Data\Market 1 Deliveries.csv',
             'e-commerce-data-pipeline\Data\Market 2 Deliveries.csv']
    for file_path in files:
        last_processed_user = None
        last_processed_user = incremental_update(
            file_path, last_processed_user)
        output_path = os.path.join('e-commerce-data-pipeline', os.path.basename(
            file_path).split('.')[0] + '_last_processed_user.json')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as json_file:
            json.dump(last_processed_user.to_dict()
                      if last_processed_user is not None else {}, json_file)
        print("Last processed user for", file_path, ":", last_processed_user)


if __name__ == "__main__":
    main()
