import json
import pandas as pd

# Load the JSON file
file_path = r"Data/event_data.json"  # Replace with your JSON file path
with open(file_path, 'r') as file:
    json_data = json.load(file)

# Convert JSON data to a DataFrame
if isinstance(json_data, list):  # Ensure the JSON data is a list of records
    df = pd.DataFrame(json_data)

    # Save the DataFrame to an Excel file in the Output folder
    output_path = r"Output/event_data.xlsx"
    df.to_excel(output_path, index=False)
    print(f"✅ JSON data successfully saved to {output_path}")
els e:
    print("❌ The JSON data is not in a tabular format suitable for a DataFrame.")