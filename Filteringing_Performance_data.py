import pandas as pd
import json

# Load the filtered Excel file
# df = pd.read_excel(r"filtered_matching_data.xlsx")
df = pd.read_excel(r"Output\perform.xlsx")
# i need to print all the data headers
print(df.head())  # Print the first few rows to see the headers and data
# i want to print all the headers
print(df.columns.tolist())  # Print all column headers
# current form:
#   asset_name             result_to               result_from  fuel_consumption
# 0  IN A 2409  2025-01-30T04:30:00Z  2025-01-30T04:20:44.079Z               0.5
# 1  IN A 1144  2025-01-30T04:30:00Z  2025-01-30T04:19:29.802Z               0.0
# 2  IN A 2409  2025-01-30T04:45:00Z      2025-01-30T04:30:00Z               2.5
# 3  IN A 1144  2025-01-30T04:45:00Z      2025-01-30T04:30:00Z               1.5
# 4  IN EB 500  2025-01-30T04:45:00Z  2025-01-30T04:41:04.813Z               0.0

# Reorder the columns
# df = df[['asset_name', 'result_from', 'result_to', 'fuel_consumption']]

# Convert the time format for 'result_from' and 'result_to'
df['result_from'] = pd.to_datetime(df['result_from'], format='ISO8601').dt.strftime('%Y-%m-%d %H:%M:%S')
df['result_to'] = pd.to_datetime(df['result_to'], format='ISO8601').dt.strftime('%Y-%m-%d %H:%M:%S')

# Sort the DataFrame by 'asset_name'
df = df.sort_values(by=['asset_name', 'result_from'])

# output:
#     asset_name          result_from            result_to  fuel_consumption
# 823  IN A 1120  2025-02-03 04:37:02  2025-02-03 04:45:00               1.0
# 831  IN A 1120  2025-02-03 04:45:00  2025-02-03 05:00:00               1.0
# 841  IN A 1120  2025-02-03 05:00:00  2025-02-03 05:15:00               1.5
# 852  IN A 1120  2025-02-03 05:15:00  2025-02-03 05:30:00               3.5
# 865  IN A 1120  2025-02-03 05:30:00  2025-02-03 05:45:00               0.5

# Save it as excel
df.to_excel(r"updated_filtered_matching_data.xlsx", index=False)

