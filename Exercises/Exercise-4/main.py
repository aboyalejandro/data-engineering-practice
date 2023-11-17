import os
import json
import glob
import pandas as pd 

def main():

    folder_path = '/content/drive/MyDrive/data'

    def flatten_json(json_data, parent_key='', sep='_'):
        flattened_data = {}
        for key, value in json_data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                flattened_data.update(flatten_json(value, new_key, sep=sep))
            else:
                flattened_data[new_key] = value
        return flattened_data

    def load_flatten_json_files(folder_path):
        flattened_json_data = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)

                if file_path.endswith('.json'):
                    try:
                        with open(file_path, 'r') as json_file:
                            data = json.load(json_file)
                            flattened_data = flatten_json(data)
                            flattened_json_data.append(flattened_data)
                            print(f"Flattened JSON file: {file_path}")
                    except json.JSONDecodeError as e:
                        print(f"Error loading JSON file {file_path}: {e}")
                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

        return flattened_json_data

    flattened_json_data = load_flatten_json_files(folder_path)
    flattened_json_data


    # for json_file in flattened_json_data:
    #output = pd.read_json(json_file, orient='records')
    # output.to_csv(f'data/extracted_csv/{file_path}_final.csv')
    # print(f"Exported to data/extracted_csv/{file_path}_final.csv")


if __name__ == "__main__":
    main()
