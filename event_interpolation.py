import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


def parse_timestamp(timestamp_str):
    """Parse timestamp string to datetime object"""
    try:
        if pd.isna(timestamp_str):
            return None

        # Convert to string if it's not already
        timestamp_str = str(timestamp_str)

        # Handle different timestamp formats
        if 'T' in timestamp_str:
            # ISO format like "2024-01-01T09:02:41.99Z"
            timestamp_str = timestamp_str.replace('Z', '').replace('T', ' ')

        # Try parsing with pandas
        parsed = pd.to_datetime(timestamp_str, errors='coerce')
        if pd.notna(parsed):
            return parsed.timestamp()  # Convert to Unix timestamp for interpolation

        return None
    except:
        return None


def analyze_data_quality(df, lat_col, lon_col, time_col):
    """Analyze the data quality and provide diagnostics"""
    print("\n=== DATA QUALITY ANALYSIS ===")

    total_rows = len(df)
    print(f"Total rows: {total_rows:,}")

    # Check coordinate columns
    valid_lat = df[lat_col].notna().sum()
    valid_lon = df[lon_col].notna().sum()
    valid_coords = ((df[lat_col].notna()) & (df[lon_col].notna())).sum()

    print(f"Valid latitude values: {valid_lat:,} ({valid_lat / total_rows * 100:.1f}%)")
    print(f"Valid longitude values: {valid_lon:,} ({valid_lon / total_rows * 100:.1f}%)")
    print(f"Valid coordinate pairs: {valid_coords:,} ({valid_coords / total_rows * 100:.1f}%)")

    # Check timestamp column
    if time_col in df.columns:
        valid_timestamps = df[time_col].notna().sum()
        print(f"Valid timestamps: {valid_timestamps:,} ({valid_timestamps / total_rows * 100:.1f}%)")

        # Check for coordinate + timestamp combinations
        df_temp = df.copy()
        df_temp['timestamp_numeric'] = df_temp[time_col].apply(parse_timestamp)
        valid_time_coords = ((df_temp[lat_col].notna()) &
                             (df_temp[lon_col].notna()) &
                             (df_temp['timestamp_numeric'].notna())).sum()
        print(
            f"Valid coordinate+timestamp combinations: {valid_time_coords:,} ({valid_time_coords / total_rows * 100:.1f}%)")

        if valid_time_coords > 0:
            # Show sample of valid data
            sample_valid = df_temp[(df_temp[lat_col].notna()) &
                                   (df_temp[lon_col].notna()) &
                                   (df_temp['timestamp_numeric'].notna())].head(3)
            print(f"\nSample of valid coordinate data:")
            print(sample_valid[[time_col, lat_col, lon_col]].to_string(index=False))

    # Check for patterns in missing data
    print(f"\n=== MISSING DATA PATTERNS ===")
    missing_both = ((df[lat_col].isna()) & (df[lon_col].isna())).sum()
    missing_lat_only = ((df[lat_col].isna()) & (df[lon_col].notna())).sum()
    missing_lon_only = ((df[lat_col].notna()) & (df[lon_col].isna())).sum()

    print(f"Missing both lat & lon: {missing_both:,}")
    print(f"Missing latitude only: {missing_lat_only:,}")
    print(f"Missing longitude only: {missing_lon_only:,}")

    return valid_coords, valid_time_coords if time_col in df.columns else 0


def interpolate_by_groups(df, lat_col, lon_col, time_col, group_col=None):
    """
    Interpolate coordinates, optionally grouping by a column (like asset_id)
    """
    if group_col and group_col in df.columns:
        print(f"\nGrouping by {group_col} for interpolation...")
        groups = df[group_col].unique()
        print(f"Found {len(groups)} unique groups")

        interpolated_dfs = []
        successful_groups = 0

        for group in groups:
            group_df = df[df[group_col] == group].copy()
            interpolated_group = interpolate_single_group(group_df, lat_col, lon_col, time_col)

            if interpolated_group is not None:
                interpolated_dfs.append(interpolated_group)
                successful_groups += 1

        print(f"Successfully interpolated {successful_groups} out of {len(groups)} groups")

        if interpolated_dfs:
            return pd.concat(interpolated_dfs, ignore_index=True)

    # If no grouping or grouping failed, try interpolating the entire dataset
    return interpolate_single_group(df, lat_col, lon_col, time_col)


def interpolate_single_group(df, lat_col, lon_col, time_col):
    """Interpolate coordinates for a single group of data"""
    df_work = df.copy()

    # Parse timestamps
    df_work['timestamp_numeric'] = df_work[time_col].apply(parse_timestamp)

    # Get rows with valid coordinates and timestamps
    valid_mask = (df_work[lat_col].notna() &
                  df_work[lon_col].notna() &
                  df_work['timestamp_numeric'].notna())

    valid_coords = df_work[valid_mask]

    if len(valid_coords) < 2:
        return None

    # Sort by timestamp
    valid_coords = valid_coords.sort_values('timestamp_numeric')

    try:
        # Create interpolation functions
        lat_interp = interp1d(
            valid_coords['timestamp_numeric'],
            valid_coords[lat_col],
            kind='linear',
            bounds_error=False,
            fill_value='extrapolate'
        )

        lon_interp = interp1d(
            valid_coords['timestamp_numeric'],
            valid_coords[lon_col],
            kind='linear',
            bounds_error=False,
            fill_value='extrapolate'
        )

        # Find rows that need interpolation
        needs_interpolation = ((df_work[lat_col].isna() | df_work[lon_col].isna()) &
                               df_work['timestamp_numeric'].notna())

        interpolated_count = 0

        # Interpolate missing values
        for idx in df_work[needs_interpolation].index:
            timestamp_val = df_work.loc[idx, 'timestamp_numeric']

            if pd.isna(df_work.loc[idx, lat_col]):
                df_work.loc[idx, lat_col] = lat_interp(timestamp_val)
                interpolated_count += 1

            if pd.isna(df_work.loc[idx, lon_col]):
                df_work.loc[idx, lon_col] = lon_interp(timestamp_val)

        if interpolated_count > 0:
            print(f"Interpolated {interpolated_count} coordinate values")

        return df_work.drop('timestamp_numeric', axis=1)

    except Exception as e:
        print(f"Interpolation failed: {e}")
        return None


def process_excel_file(input_filename='event.xlsx', output_filename='event_interpolated.xlsx'):
    """Main function to process the Excel file"""
    try:
        print(f"Reading Excel file: {input_filename}")
        df = pd.read_excel(input_filename)

        print(f"File loaded successfully. Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        # Find coordinate columns
        lat_col = lon_col = time_col = None

        for col in df.columns:
            col_lower = col.lower()
            if 'latitude' in col_lower and lat_col is None:
                lat_col = col
            elif 'longitude' in col_lower and lon_col is None:
                lon_col = col
            elif any(word in col_lower for word in ['time', 'occurred', 'timestamp']) and time_col is None:
                time_col = col

        if lat_col is None or lon_col is None:
            print("Error: Could not find latitude and longitude columns")
            return

        if time_col is None:
            print("Warning: Could not find timestamp column. Looking for 'occurred_at'...")
            if 'occurred_at' in df.columns:
                time_col = 'occurred_at'
            else:
                print("Error: No timestamp column found for interpolation")
                return

        print(f"Using columns - Time: {time_col}, Latitude: {lat_col}, Longitude: {lon_col}")

        # Analyze data quality
        valid_coords, valid_time_coords = analyze_data_quality(df, lat_col, lon_col, time_col)

        if valid_time_coords < 2:
            print(f"\n❌ Cannot perform interpolation:")
            print(f"   - Need at least 2 rows with valid coordinates AND timestamps")
            print(f"   - Found only {valid_time_coords} such rows")
            print(f"\n💡 Suggestions:")
            print(f"   1. Check if timestamps are in correct format")
            print(f"   2. Try grouping by asset_id or device_id")
            print(f"   3. Use forward/backward fill instead of interpolation")

            # Try alternative approaches
            return try_alternative_approaches(df, lat_col, lon_col, output_filename)

        # Try interpolation with grouping
        print(f"\n=== ATTEMPTING INTERPOLATION ===")
        result_df = interpolate_by_groups(df, lat_col, lon_col, time_col, 'asset_id')

        if result_df is None:
            print("Grouped interpolation failed. Trying without grouping...")
            result_df = interpolate_by_groups(df, lat_col, lon_col, time_col)

        if result_df is None:
            print("All interpolation methods failed. Using fallback approach...")
            return try_alternative_approaches(df, lat_col, lon_col, output_filename)

        # Save results
        print(f"\nSaving results to: {output_filename}")
        result_df.to_excel(output_filename, index=False)

        # Final statistics
        final_missing_lat = result_df[lat_col].isna().sum()
        final_missing_lon = result_df[lon_col].isna().sum()

        print(f"\n=== FINAL RESULTS ===")
        print(f"Missing latitude values: {final_missing_lat:,}")
        print(f"Missing longitude values: {final_missing_lon:,}")
        print(f"✅ File saved successfully!")

    except Exception as e:
        print(f"Error: {e}")


def try_alternative_approaches(df, lat_col, lon_col, output_filename):
    """Try alternative approaches when interpolation fails"""
    print(f"\n=== TRYING ALTERNATIVE APPROACHES ===")

    df_alt = df.copy()
    original_missing_lat = df_alt[lat_col].isna().sum()
    original_missing_lon = df_alt[lon_col].isna().sum()

    # Method 1: Forward fill then backward fill
    print("1. Trying forward fill + backward fill...")
    df_alt[lat_col] = df_alt[lat_col].fillna(method='ffill').fillna(method='bfill')
    df_alt[lon_col] = df_alt[lon_col].fillna(method='ffill').fillna(method='bfill')

    missing_after_fill = df_alt[lat_col].isna().sum()
    filled_count = original_missing_lat - missing_after_fill

    if filled_count > 0:
        print(f"   ✅ Filled {filled_count:,} coordinate pairs using forward/backward fill")

        # Save the result
        alt_filename = output_filename.replace('.xlsx', '_filled.xlsx')
        df_alt.to_excel(alt_filename, index=False)
        print(f"   💾 Saved to: {alt_filename}")
        return df_alt
    else:
        print("   ❌ Forward/backward fill didn't help")

    print(f"\n❌ No interpolation possible with current data")
    return None


if __name__ == "__main__":
    # Run the main function
    # You can change the filenames here if needed
    input_file = r"Output/event_data_10000_snippet.xlsx"  # Change this to your input file name
    output_file = r"Output/event_snippet_10000_interpolated.xlsx"  # Change this to desired output file name

    process_excel_file(input_file, output_file)