import pandas as pd

# Load the Excel file
df = pd.read_excel(r"C:\Users\anand\Downloads\CW-export_2.25.xlsx")

# Clean up column names
df.columns = df.columns.str.strip()

# Filter for rows where 'containerType' contains 'FLC' or 'ARC'
filtered_df = df[df['containerType'].astype(str).str.contains('FLC|ARC', na=False)]

# Define the columns you want to extract
columns_to_extract = [
    'date', 'start', 'end', 'truck', 'clientAddress',
    'truckId', 'containerType', 'completionLongitude', 'completionLatitude', 'tourNo'
]

# Extract just those columns from the filtered data
extracted_df = filtered_df[columns_to_extract]
print(extracted_df.head())
# Save to a new Excel or CSV file
extracted_df.to_excel(r"extracted_FLC_ARC_data.xlsx", index=False)
# Or CSV:
# extracted_df.to_csv(r"C:\Users\anand\Downloads\extracted_FLC_ARC_data.csv", index=False)

print("✅ Extracted data with 'FLC' or 'ARC' saved successfully.")


