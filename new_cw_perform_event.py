import pandas as pd
import numpy as np
from datetime import datetime
import ast
import warnings

warnings.filterwarnings('ignore')


def parse_asset_ids(asset_id_str):
    """
    Parse asset IDs from string format like "['id1', 'id2']" to list
    """
    try:
        if pd.isna(asset_id_str):
            return []

        if isinstance(asset_id_str, str):
            # Clean the string and handle different formats
            cleaned = asset_id_str.strip()

            # Handle string representation of list like "['id1', 'id2']"
            if cleaned.startswith("['") and cleaned.endswith("']"):
                try:
                    return ast.literal_eval(cleaned)
                except:
                    # If ast fails, try manual parsing
                    cleaned = cleaned[2:-2]  # Remove [' and ']
                    return [cleaned] if cleaned else []

            # Handle list format like ['id1', 'id2']
            elif cleaned.startswith('[') and cleaned.endswith(']'):
                try:
                    return ast.literal_eval(cleaned)
                except:
                    # Manual parsing fallback
                    cleaned = cleaned[1:-1]  # Remove [ and ]
                    if cleaned:
                        # Split by comma and clean quotes
                        ids = [id_.strip().strip("'\"") for id_ in cleaned.split(',')]
                        return [id_ for id_ in ids if id_]
                    return []

            # Single asset ID
            else:
                return [cleaned]

        # If it's already a list or other type
        return asset_id_str if isinstance(asset_id_str, list) else [str(asset_id_str)]

    except Exception as e:
        print(f"Error parsing asset ID '{asset_id_str}': {e}")
        return []


def parse_event_timestamp(timestamp_str):
    """
    Parse event timestamp from ISO format like '2025-01-31T09:02:41.99Z'
    """
    try:
        if pd.isna(timestamp_str):
            return pd.NaT

        # Handle different timestamp formats
        timestamp_str = str(timestamp_str).strip()

        # Remove 'Z' if present
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1]

        # Try different parsing methods
        try:
            return pd.to_datetime(timestamp_str, format='%Y-%m-%dT%H:%M:%S.%f')
        except:
            try:
                return pd.to_datetime(timestamp_str, format='%Y-%m-%dT%H:%M:%S')
            except:
                return pd.to_datetime(timestamp_str)
    except:
        return pd.NaT


def merge_performance_event_data(performance_file, event_file, output_file):
    """
    Merge performance and event data based on asset ID and time intervals
    """
    print("=" * 50)
    print("STARTING DATA MERGE PROCESS")
    print("=" * 50)

    # Read the data files
    print("Reading performance data...")
    try:
        perf_data = pd.read_excel(performance_file)
        print(f"Performance data shape: {perf_data.shape}")
    except Exception as e:
        print(f"Error reading performance data: {e}")
        return

    print("Reading event data...")
    try:
        event_data = pd.read_excel(event_file)
        event_data['occurred_at'] = pd.to_datetime(event_data['occurred_at'], format='ISO8601').dt.strftime('%Y-%m-%d %H:%M:%S')
        print(f"Event data shape: {event_data.shape}")
    except Exception as e:
        print(f"Error reading event data: {e}")
        return

    # Parse datetime columns
    print("\nProcessing datetime columns...")

    # Parse performance data timestamps
    if 'start' in perf_data.columns:
        perf_data['start_parsed'] = pd.to_datetime(perf_data['start'], errors='coerce')
    if 'end' in perf_data.columns:
        perf_data['end_parsed'] = pd.to_datetime(perf_data['end'], errors='coerce')

    # Parse event timestamps
    if 'occurred_at' in event_data.columns:
        print("Parsing event timestamps...")
        event_data['occurred_at_parsed'] = event_data['occurred_at'].apply(parse_event_timestamp)

        # Check how many timestamps were successfully parsed
        valid_timestamps = event_data['occurred_at_parsed'].notna().sum()
        print(f"Successfully parsed {valid_timestamps} out of {len(event_data)} event timestamps")
    else:
        print("Warning: 'occurred_at' column not found in event data")
        return

    # Parse asset IDs in performance data
    print("\nProcessing asset IDs...")
    if 'perf_asset_ids' in perf_data.columns:
        perf_data['asset_ids_parsed'] = perf_data['perf_asset_ids'].apply(parse_asset_ids)
        print("Using 'perf_asset_ids' from performance data")
    else:
        print("Warning: 'perf_asset_ids' column not found in performance data")
        return

    # Check asset ID column in event data
    if 'asset_id' not in event_data.columns:
        print("Warning: 'asset_id' column not found in event data")
        return

    print(
        f"Unique assets in performance data: {len(set([asset for sublist in perf_data['asset_ids_parsed'] for asset in sublist]))}")
    print(f"Unique assets in event data: {event_data['asset_id'].nunique()}")

    # Debug: Show sample asset IDs from both datasets
    print("\n--- DEBUGGING ASSET IDs ---")
    perf_assets = set([asset for sublist in perf_data['asset_ids_parsed'] for asset in sublist])
    event_assets = set(event_data['asset_id'].unique())

    print(f"Sample performance asset IDs:")
    for i, asset in enumerate(list(perf_assets)[:5]):
        print(f"  {i + 1}. {asset}")

    print(f"Sample event asset IDs:")
    for i, asset in enumerate(list(event_assets)[:5]):
        print(f"  {i + 1}. {asset}")

    # Check for any overlapping asset IDs
    overlapping_assets = perf_assets.intersection(event_assets)
    print(f"Overlapping asset IDs found: {len(overlapping_assets)}")
    if overlapping_assets:
        print("Overlapping assets:")
        for asset in list(overlapping_assets)[:5]:
            print(f"  - {asset}")
    else:
        print("NO OVERLAPPING ASSET IDs FOUND!")
        print("This explains why no matches were made.")

    # Debug: Show timestamp ranges
    print("\n--- DEBUGGING TIMESTAMPS ---")
    perf_start_min = perf_data['start_parsed'].min()
    perf_start_max = perf_data['start_parsed'].max()
    perf_end_min = perf_data['end_parsed'].min()
    perf_end_max = perf_data['end_parsed'].max()

    event_min = event_data['occurred_at_parsed'].min()
    event_max = event_data['occurred_at_parsed'].max()

    print(f"Performance time range: {perf_start_min} to {perf_end_max}")
    print(f"Event time range: {event_min} to {event_max}")
    print("--- END DEBUGGING ---\n")

    # Create merged dataset
    print("\nStarting merge process...")
    merged_records = []

    # Group performance data by asset for faster lookup
    perf_by_asset = {}
    for idx, row in perf_data.iterrows():
        for asset_id in row['asset_ids_parsed']:
            if asset_id not in perf_by_asset:
                perf_by_asset[asset_id] = []
            perf_by_asset[asset_id].append({
                'index': idx,
                'start': row.get('start_parsed'),
                'end': row.get('end_parsed'),
                'data': row
            })

    print(f"Performance data organized for {len(perf_by_asset)} assets")

    # Process each event record
    processed_events = 0
    matched_events = 0

    for idx, event_row in event_data.iterrows():
        processed_events += 1
        if processed_events % 1000 == 0:
            print(f"Processed {processed_events} events, matched {matched_events}")

        event_asset_id = event_row['asset_id']
        event_timestamp = event_row['occurred_at_parsed']

        # Skip if timestamp is invalid
        if pd.isna(event_timestamp):
            continue

        # Find matching performance records
        if event_asset_id in perf_by_asset:
            for perf_record in perf_by_asset[event_asset_id]:
                perf_start = perf_record['start']
                perf_end = perf_record['end']

                # Skip if performance timestamps are invalid
                if pd.isna(perf_start) or pd.isna(perf_end):
                    continue

                # Check if event timestamp falls within performance interval
                if perf_start <= event_timestamp <= perf_end:
                    # Create merged record
                    merged_record = {}

                    # Add all event data with 'event_' prefix
                    for col in event_data.columns:
                        merged_record[f'event_{col}'] = event_row[col]

                    # Add all performance data with 'perf_' prefix (if not already prefixed)
                    for col in perf_data.columns:
                        if col.startswith('perf_'):
                            merged_record[col] = perf_record['data'][col]
                        else:
                            merged_record[f'perf_{col}'] = perf_record['data'][col]

                    merged_records.append(merged_record)
                    matched_events += 1
                    break  # Only match with first valid performance interval

    print(f"\nMerge complete!")
    print(f"Total events processed: {processed_events}")
    print(f"Total events matched: {matched_events}")
    print(f"Total merged records: {len(merged_records)}")

    if not merged_records:
        print("No matching records found. Please check:")
        print("1. Asset IDs match between datasets")
        print("2. Time ranges overlap")
        print("3. Timestamp formats are correct")

        # Additional debugging if no matches
        print("\nDETAILED DIAGNOSIS:")

        # Check if asset IDs were parsed correctly
        sample_perf_raw = perf_data['perf_asset_ids'].iloc[0] if len(perf_data) > 0 else None
        sample_perf_parsed = perf_data['asset_ids_parsed'].iloc[0] if len(perf_data) > 0 else None
        print(f"Sample raw performance asset ID: {sample_perf_raw}")
        print(f"Sample parsed performance asset ID: {sample_perf_parsed}")

        # Show a few examples of potential matches to debug
        if len(perf_data) > 0 and len(event_data) > 0:
            print(f"\nTrying manual match check on first few records...")
            for i in range(min(30, len(perf_data))):
                perf_assets = perf_data['asset_ids_parsed'].iloc[i]
                perf_start = perf_data['start_parsed'].iloc[i]
                perf_end = perf_data['end_parsed'].iloc[i]
                print(f"Perf record {i}: assets={perf_assets}, time={perf_start} to {perf_end}")

                # Check if any event could match
                matching_events = event_data[event_data['asset_id'].isin(perf_assets)]
                if len(matching_events) > 0:
                    print(f"  Found {len(matching_events)} events with matching asset IDs")
                    # Check time overlap
                    time_matches = matching_events[
                        (matching_events['occurred_at_parsed'] >= perf_start) &
                        (matching_events['occurred_at_parsed'] <= perf_end)
                        ]
                    print(f"  Found {len(time_matches)} events within time range")
                else:
                    print(f"  No events found with matching asset IDs")

        return

    # Create merged DataFrame
    merged_df = pd.DataFrame(merged_records)

    # Sort by event timestamp
    if 'event_occurred_at_parsed' in merged_df.columns:
        merged_df = merged_df.sort_values('event_occurred_at_parsed')

    # Save to Excel
    print(f"\nSaving merged data to {output_file}...")
    try:
        merged_df.to_excel(output_file, index=False)
        print(f"Successfully saved {len(merged_df)} records to {output_file}")
        print(f"Merged data shape: {merged_df.shape}")

        # Display sample of merged data
        print(f"\nSample merged data columns:")
        print(merged_df.columns.tolist()[:10], "...")  # Show first 10 columns

    except Exception as e:
        print(f"Error saving file: {e}")
        return

    # Print summary statistics
    print(f"\nSUMMARY:")
    print(f"- Performance records: {len(perf_data)}")
    print(f"- Event records: {len(event_data)}")
    print(f"- Merged records: {len(merged_df)}")
    print(f"- Match rate: {matched_events / processed_events * 100:.2f}% of valid events")

    return merged_df



# Main execution
if __name__ == "__main__":
    # File paths
    performance_file = r"cw_perform_merged.xlsx"
    event_file = r"Output/event_interpolated.xlsx"
    output_file = "perform_event_merged.xlsx"

    # Run the merge
    result = merge_performance_event_data(performance_file, event_file, output_file)

    if result is not None:
        print("\n" + "=" * 50)
        print("MERGE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("MERGE FAILED - CHECK ERROR MESSAGES ABOVE")
        print("=" * 50)