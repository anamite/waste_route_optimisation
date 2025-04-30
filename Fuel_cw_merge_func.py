import pandas as pd
from icecream import ic

# Customizing prefix

def compute_fuel_for_cw(cw_file='CW.xlsx', perform_file='Perform.xlsx', output_file='CW_Updated.xlsx'):
    # Read data
    df_cw = pd.read_excel(cw_file)
    df_perform = pd.read_excel(perform_file)

    # Convert to datetime
    df_cw['start'] = pd.to_datetime(df_cw['start'])
    df_cw['end'] = pd.to_datetime(df_cw['end'])
    df_perform['result_from'] = pd.to_datetime(df_perform['result_from'])
    df_perform['result_to'] = pd.to_datetime(df_perform['result_to'])

    # Add a row ID to df_cw to help identify each row later
    df_cw['cw_row_id'] = df_cw.index

    # We'll create a cross-join only for matching truck vs asset_name:
    # 1) rename or create a key in df_cw
    df_cw['_truck_key'] = df_cw['truck']
    df_perform['_truck_key'] = df_perform['asset_name']

    # 2) add a dummy column for cross-join
    df_cw['_dummy'] = 1
    df_perform['_dummy'] = 1

    # 3) merge on both _truck_key and _dummy
    #    so we only get pairs of rows that have the same truck, but cross-join
    merged = pd.merge(
        df_cw, df_perform,
        on=['_truck_key', '_dummy'],
        how='left',
        suffixes=('_cw', '_perf')
    )
    # save the merged file to a new excel name_ "MERGED" and check if the data is merged properly
    # merged.to_excel('MERGED.xlsx', index=False)

    # Define overlap function
    def get_overlap_minutes(row):
        start_cw = row['start']
        end_cw = row['end']
        start_pf = row['result_from']
        end_pf = row['result_to']

        # If any are null, no overlap
        if pd.isnull(start_pf) or pd.isnull(end_pf):
            return 0

        # if (end_pf - start_pf) > (end_cw - start_cw):
        #     print(f"Perform interval longer than CW interval: {row}")

        overlap_start = max(start_cw, start_pf)
        overlap_end = min(end_cw, end_pf)
        overlap = (overlap_end - overlap_start).total_seconds() / 60.0
        # if overlap > 0:
        #     ic(row)
        #     ic(overlap)
        return overlap if overlap > 0 else 0

    merged['overlap_minutes'] = merged.apply(get_overlap_minutes, axis=1)

    # Perform interval length
    merged['perform_interval_minutes'] = (
            (merged['result_to'] - merged['result_from'])
            .dt.total_seconds() / 60.0
    )

    # Overlap fraction
    merged['overlap_fraction'] = merged.apply(
        lambda r: (r['overlap_minutes'] / r['perform_interval_minutes'])
        if r['perform_interval_minutes'] and r['perform_interval_minutes'] > 0
        else 0,
        axis=1
    )

    # Partial fuel
    merged['partial_fuel'] = merged['overlap_fraction'] * merged['fuel_consumption'].fillna(0)

    # Sum partial fuel by cw_row_id
    fuel_sums = merged.groupby('cw_row_id')['partial_fuel'].sum().reset_index()
    fuel_sums.rename(columns={'partial_fuel': 'total_fuel_for_CW'}, inplace=True)

    # Merge back to df_cw
    df_cw = pd.merge(df_cw, fuel_sums, on='cw_row_id', how='left')

    # Clean up helper columns
    df_cw.drop(columns=['_truck_key', '_dummy', 'cw_row_id'], inplace=True)

    # Write to Excel
    df_cw.to_excel(output_file, index=False)


if __name__ == "__main__":
    compute_fuel_for_cw(
        cw_file=r'latilong.xlsx',
        perform_file=r'updated_filtered_matching_data.xlsx',
        output_file='CW_Updated.xlsx'
    )








# def compute_fuel_for_cw(cw_file='CW.xlsx', perform_file='Perform.xlsx', output_file='CW_Updated.xlsx'):
#     # 1. Read the two Excel files
#     df_cw = pd.read_excel(cw_file)
#     df_perform = pd.read_excel(perform_file)
#
#     # 2. Convert the time columns to datetime
#     df_cw['start'] = pd.to_datetime(df_cw['start'])
#     df_cw['end'] = pd.to_datetime(df_cw['end'])
#
#     df_perform['result_from'] = pd.to_datetime(df_perform['result_from'])
#     df_perform['result_to'] = pd.to_datetime(df_perform['result_to'])
#
#     # 3. We only need to merge rows that have the same truck/asset_name.
#     #    We'll create a cross-join for those rows.
#     #    Method: add a _key = truck to df_cw and _key = asset_name to df_perform, then merge.
#     df_cw['_merge_key'] = df_cw['truck']
#     df_perform['_merge_key'] = df_perform['asset_name']
#
#     #    This will pair up each CW row with each Perform row for the same truck.
#     merged = pd.merge(df_cw, df_perform, on='_merge_key', how='left', suffixes=('_cw', '_perf'))
#
#     # 4. Define a function to compute overlap in minutes between two intervals
#     def get_overlap_minutes(row):
#         start_cw = row['start_cw']
#         end_cw = row['end_cw']
#         start_pf = row['result_from']
#         end_pf = row['result_to']
#
#         if pd.isnull(start_pf) or pd.isnull(end_pf):
#             # If there's no matching Perform data (NaN), overlap is zero
#             return 0
#
#         overlap_start = max(start_cw, start_pf)
#         overlap_end = min(end_cw, end_pf)
#
#         overlap = (overlap_end - overlap_start).total_seconds() / 60.0
#         return overlap if overlap > 0 else 0
#
#     # Apply the function
#     merged['overlap_minutes'] = merged.apply(get_overlap_minutes, axis=1)
#
#     # 5. Calculate how long each Perform interval is (in minutes)
#     merged['perform_interval_minutes'] = (
#             (merged['result_to'] - merged['result_from'])
#             .dt.total_seconds() / 60.0
#     )
#
#     # 6. Fraction of the Perform interval that overlaps with CW
#     #    If there's no perform interval (NaN or zero length), set fraction to 0
#     def get_overlap_fraction(row):
#         if row['perform_interval_minutes'] and row['perform_interval_minutes'] > 0:
#             return row['overlap_minutes'] / row['perform_interval_minutes']
#         else:
#             return 0
#
#     merged['overlap_fraction'] = merged.apply(get_overlap_fraction, axis=1)
#
#     # 7. Multiply fraction by the perform file's fuel_consumption
#     #    If fuel_consumption is NaN, treat as 0
#     merged['partial_fuel'] = merged['overlap_fraction'] * merged['fuel_consumption'].fillna(0)
#
#     # 8. Now we want the sum of partial fuel usage for each CW row.
#     #    We'll identify each original CW row by its index in df_cw before the merge.
#     merged['cw_row_id'] = merged.index
#     # Actually, we want the original row index from df_cw specifically:
#     # We'll do that by:
#     df_cw['cw_row_id'] = df_cw.index
#
#     # But after merging, let's fix that:
#     # We can re-merge so we keep track of which row is which:
#     # Actually, a simpler approach: we do the cross-join in a different way:
#     # But let's just group by the columns that uniquely identify the CW row.
#     # We'll assume 'cw_row_id' from df_cw is enough.
#
#     # Because we lost the original index in the merge, let's do it systematically:
#     # 1) Add cw_row_id to df_cw
#     # 2) Merge on both _merge_key and cw_row_id (the second is a dummy cross-join key)
#
#     # For clarity, let's fix that approach now:
#     # We'll do it step by step again, more simply: