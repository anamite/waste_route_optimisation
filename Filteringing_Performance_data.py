import pandas as pd
import json

# Load the filtered Excel file
df = pd.read_excel(r"filtered_matching_data.xlsx")

# Reorder the columns
df = df[['asset_name', 'result_from', 'result_to', 'fuel_consumption']]

# Convert the time format for 'result_from' and 'result_to'
df['result_from'] = pd.to_datetime(df['result_from'], format='ISO8601').dt.strftime('%Y-%m-%d %H:%M:%S')
df['result_to'] = pd.to_datetime(df['result_to'], format='ISO8601').dt.strftime('%Y-%m-%d %H:%M:%S')

# Sort the DataFrame by 'asset_name'
df = df.sort_values(by=['asset_name', 'result_from'])

# Save the updated DataFrame to a new Excel file
df.to_excel(r"updated_filtered_matching_data.xlsx", index=False)

print("✅ Updated data saved successfully.")