import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the Excel file
df = pd.read_excel(r"Data\CW-export_2.25.xlsx")

# Clean up column names
df.columns = df.columns.str.strip()

# Filter for rows where 'containerType' contains 'FLC' or 'ARC'
filtered_df = df[df['containerType'].astype(str).str.contains('FLC|ARC', na=False)]

# Define the columns you want to extract
columns_to_extract = [
    'date', 'start', 'end', 'duration', 'employeeIds', 'drivers', 'area', 'truck', 'orderId', 'orderLink',
    'contractId', 'siteId', 'costcenter', 'LE-KST', 'trailerCostcenter', 'costcenter-lohn', 'logisticProcess',
    'timeAtDisposalSite', 'timeAtClient', 'enteredClientArea', 'leftClientArea', 'enterDisposalSite',
    'leaveDisposalSite', 'estimatedDuration', 'breakDuration', 'issueWaitingTimes', 'resume', 'paused', 'tasks',
    'issues', 'clientAddress', 'disposalSite', 'summDistance', 'credit', 'la', 'bs', 'wds_id', 'timeDifference',
    'allRestingTime', 'allReportedResting', 'freeTimeWithoutTransport', 'restingOverrideDifference', 'summMoveTime',
    'summStandTime', 'summCovered', 'nonOrderTime', 'approvalTime', 'netWorkingHours', 'startWorking', 'endWorking',
    'employeeInternalIds', 'kaba-export', 'workdayApproved', 'approvalNote', 'dayCredit', 'reportedRestingTruncated',
    'noOfOrders', 'truckId', 'deliveryId', 'contractInternalId', 'siteInternalId', 'vehicleType', 'xuId', 'deliverId',
    'orderInternalId', 'weight', 'wasteType', 'containerType', 'initiator', 'customersInternalReference',
    'completionLongitude', 'completionLatitude', 'leftLifterCount', 'rightLifterCount', '4wheelActionCount',
    'tourNo', 'tourDesc'
]


# Extract just those columns from the filtered data
extracted_df = filtered_df[columns_to_extract]
print(extracted_df.head())
# i want to print all the colms for first 10
print(extracted_df.head(10).to_string())  # Print all columns for the first 10 rows

import requests
from requests.structures import CaseInsensitiveDict


# Function to get coordinates from the Geoapify API
def get_coordinates(address):
    url = f"https://api.geoapify.com/v1/geocode/search?text={address}&apiKey=786433156f5042d68cd42f0408e57c3f"
    # url = f"https://api.geoapify.com/v1/geocode/search?text={address}&apiKey=6e3e795ea3d74df3ba5f0f9f00de2e80"
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if data['features']:
            longitude = data['features'][0]['properties']['lon']
            latitude = data['features'][0]['properties']['lat']
            return longitude, latitude
    return None, None


# Function to update the DataFrame with coordinates
def update_coordinates(row):
    if pd.isna(row['completionLongitude']) or pd.isna(row['completionLatitude']) or \
       row['completionLongitude'] == 0 or row['completionLatitude'] == 0:
        longitude, latitude = get_coordinates(row['clientAddress'])
    return row

# Testing if the code would work for a single row
for index, row in extracted_df.iterrows():
    if pd.isna(row['completionLongitude']) or pd.isna(row['completionLatitude']) or \
       row['completionLongitude'] == 0 or row['completionLatitude'] == 0:
        longitude, latitude = get_coordinates(row['clientAddress'])
        if longitude is not None and latitude is not None:
            extracted_df.at[index, 'completionLongitude'] = longitude
            extracted_df.at[index, 'completionLatitude'] = latitude
            print(extracted_df.loc[index])
            break


# Iterate through the rows and update missing or zero coordinates
for index, row in extracted_df.iterrows():
    if pd.isna(row['completionLongitude']) or pd.isna(row['completionLatitude']) or \
       row['completionLongitude'] == 0 or row['completionLatitude'] == 0:
        longitude, latitude = get_coordinates(row['clientAddress'])
        if longitude is not None and latitude is not None:
            extracted_df.at[index, 'completionLongitude'] = longitude
            extracted_df.at[index, 'completionLatitude'] = latitude

# Save the updated DataFrame to a new file
latilong = extracted_df
latilong.to_excel(r"Output/latilong.xlsx", index=False)

