import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Set page configuration
st.set_page_config(
    page_title="Vehicle Performance Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #ff6b6b;
    }
    .stSelectbox > label {
        font-weight: bold;
    }
    .main-header {
        color: #2c3e50;
        text-align: center;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    """Load and preprocess the performance data"""
    try:
        # Load the Excel file
        df = pd.read_excel(r"C:\Users\anand\PycharmProjects\PROJECT SEM EGM\perform_event_merged.xlsx")

        # Data preprocessing
        df['event_occurred_at_parsed'] = pd.to_datetime(df['event_occurred_at_parsed'], errors='coerce')
        df['perf_start_parsed'] = pd.to_datetime(df['perf_start_parsed'], errors='coerce')
        df['perf_end_parsed'] = pd.to_datetime(df['perf_end_parsed'], errors='coerce')

        # Remove rows with invalid coordinates
        df = df.dropna(subset=['event_latitude', 'event_longitude'])
        df = df[(df['event_latitude'] != 0) & (df['event_longitude'] != 0)]

        # Filter out extreme coordinate values
        df = df[(df['event_latitude'].between(-90, 90)) &
                (df['event_longitude'].between(-180, 180))]

        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None


def sample_data_for_map(df, max_points=1000):
    """Sample data points for map visualization to improve performance"""
    if len(df) <= max_points:
        return df

    # Sample data while preserving temporal distribution
    df_sorted = df.sort_values('event_occurred_at_parsed')
    step = len(df) // max_points
    sampled_df = df_sorted.iloc[::step].copy()

    # Always include first and last points
    if len(sampled_df) > 0:
        first_point = df_sorted.iloc[0:1]
        last_point = df_sorted.iloc[-1:]
        sampled_df = pd.concat([first_point, sampled_df, last_point]).drop_duplicates()

    return sampled_df


def create_route_map(df, truck_name):
    """Create an interactive map with route visualization"""
    if df.empty:
        return None

    # Sample data for better performance
    map_df = sample_data_for_map(df, max_points=500)

    # Calculate map center
    center_lat = map_df['event_latitude'].mean()
    center_lon = map_df['event_longitude'].mean()

    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles='OpenStreetMap'
    )

    # Add route line
    if len(map_df) > 1:
        coordinates = [[row['event_latitude'], row['event_longitude']]
                       for _, row in map_df.iterrows()]

        folium.PolyLine(
            coordinates,
            color='blue',
            weight=3,
            opacity=0.7,
            popup=f"Route for {truck_name}"
        ).add_to(m)

    # Add markers for data points
    for idx, row in map_df.iterrows():
        # Create popup content with key metrics
        popup_content = f"""
        <div style="font-family: Arial; font-size: 12px; min-width: 200px;">
            <h4 style="color: #2c3e50; margin-bottom: 10px;">🚛 {truck_name}</h4>
            <hr style="margin: 5px 0;">
            <b>📅 Time:</b> {row['event_occurred_at_parsed'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['event_occurred_at_parsed']) else 'N/A'}<br>
            <b>⛽ Fuel Level:</b> {row['event_fuel_level']:.1f}% <br>
            <b>🏃 Speed:</b> {row['event_speed']:.1f} km/h<br>
            <b>🔧 Engine Speed:</b> {row['event_engine_speed']:.0f} RPM<br>
            <b>📏 Mileage:</b> {row['event_mileage']:.1f} km<br>
            <b>🌡️ Temperature:</b> {row['event_ambient_air_temperature']:.1f}°C<br>
            <b>⚖️ Weight:</b> {row['event_weight_total']:.1f} kg<br>
        </div>
        """

        # Color-code markers based on fuel level
        fuel_level = row['event_fuel_level']
        if fuel_level > 70:
            color = 'green'
            icon = 'leaf'
        elif fuel_level > 30:
            color = 'orange'
            icon = 'exclamation-triangle'
        else:
            color = 'red'
            icon = 'exclamation-circle'

        folium.Marker(
            location=[row['event_latitude'], row['event_longitude']],
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=f"Fuel: {fuel_level:.1f}% | Speed: {row['event_speed']:.1f} km/h",
            icon=folium.Icon(color=color, icon=icon, prefix='fa')
        ).add_to(m)

    # Add start and end markers
    if len(map_df) > 0:
        start_point = map_df.iloc[0]
        end_point = map_df.iloc[-1]

        folium.Marker(
            location=[start_point['event_latitude'], start_point['event_longitude']],
            popup="🏁 Start Point",
            icon=folium.Icon(color='green', icon='play', prefix='fa')
        ).add_to(m)

        folium.Marker(
            location=[end_point['event_latitude'], end_point['event_longitude']],
            popup="🏁 End Point",
            icon=folium.Icon(color='red', icon='stop', prefix='fa')
        ).add_to(m)

    return m


def main():
    st.markdown("<h1 class='main-header'>Vehicle Performance Dashboard</h1>", unsafe_allow_html=True)

    # Load data
    with st.spinner("Loading performance data..."):
        df = load_data()

    if df is None or df.empty:
        st.error("❌ No data available. Please check your performance_data.xlsx file.")
        return

    st.success(f"✅ Loaded {len(df):,} data points")

    # Sidebar filters
    st.sidebar.header("🔧 Filters")

    # Truck selection
    trucks = sorted(df['perf_truck'].dropna().unique())
    selected_truck = st.sidebar.selectbox(
        "🚛 Select Truck:",
        options=trucks,
        index=0
    )

    # Filter data by selected truck
    truck_df = df[df['perf_truck'] == selected_truck]

    # Date range filter
    if not truck_df['event_occurred_at_parsed'].isna().all():
        min_date = truck_df['event_occurred_at_parsed'].min().date()
        max_date = truck_df['event_occurred_at_parsed'].max().date()

        date_range = st.sidebar.date_input(
            "📅 Select Date Range:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        if len(date_range) == 2:
            start_date, end_date = date_range
            truck_df = truck_df[
                (truck_df['event_occurred_at_parsed'].dt.date >= start_date) &
                (truck_df['event_occurred_at_parsed'].dt.date <= end_date)
                ]

    # Additional filters
    st.sidebar.subheader("📊 Performance Filters")

    # Fuel level filter
    fuel_range = st.sidebar.slider(
        "⛽ Fuel Level Range (%):",
        min_value=0.0,
        max_value=100.0,
        value=(0.0, 100.0),
        step=5.0
    )

    truck_df = truck_df[
        (truck_df['event_fuel_level'] >= fuel_range[0]) &
        (truck_df['event_fuel_level'] <= fuel_range[1])
        ]

    # Speed filter
    if not truck_df['event_speed'].isna().all():
        max_speed = float(truck_df['event_speed'].max())
        speed_range = st.sidebar.slider(
            "🏃 Speed Range (km/h):",
            min_value=0.0,
            max_value=max_speed,
            value=(0.0, max_speed),
            step=5.0
        )

        truck_df = truck_df[
            (truck_df['event_speed'] >= speed_range[0]) &
            (truck_df['event_speed'] <= speed_range[1])
            ]

    # Driver filter
    drivers = truck_df['perf_drivers'].dropna().unique()
    if len(drivers) > 1:
        selected_driver = st.sidebar.selectbox(
            "👨‍💼 Select Driver:",
            options=['All'] + list(drivers)
        )

        if selected_driver != 'All':
            truck_df = truck_df[truck_df['perf_drivers'] == selected_driver]

    # Main content area
    if truck_df.empty:
        st.warning("⚠️ No data available for the selected filters.")
        return

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_fuel = truck_df['event_fuel_level'].mean()
        st.metric("⛽ Avg Fuel Level", f"{avg_fuel:.1f}%")

    with col2:
        avg_speed = truck_df['event_speed'].mean()
        st.metric("🏃 Avg Speed", f"{avg_speed:.1f} km/h")

    with col3:
        total_distance = truck_df['perf_summDistance'].iloc[0] if not truck_df['perf_summDistance'].isna().all() else 0
        st.metric("📏 Total Distance", f"{total_distance:.1f} km")

    with col4:
        data_points = len(truck_df)
        st.metric("📊 Data Points", f"{data_points:,}")

    # Map visualization
    st.subheader(f"🗺️ Route Map for {selected_truck}")

    # Create and display map
    route_map = create_route_map(truck_df, selected_truck)

    if route_map:
        map_data = st_folium(route_map, width=1200, height=600)

        # Display additional info about clicked points
        if map_data['last_object_clicked_popup']:
            st.info("💡 Tip: Click on map markers to see detailed information!")
    else:
        st.error("❌ Unable to create map. No valid coordinate data available.")

    # Performance charts
    st.subheader("📈 Performance Analytics")

    col1, col2 = st.columns(2)

    with col1:
        # Fuel consumption over time
        if not truck_df['event_occurred_at_parsed'].isna().all():
            fig_fuel = px.line(
                truck_df.sort_values('event_occurred_at_parsed'),
                x='event_occurred_at_parsed',
                y='event_fuel_level',
                title='⛽ Fuel Level Over Time',
                labels={'event_fuel_level': 'Fuel Level (%)', 'event_occurred_at_parsed': 'Time'}
            )
            fig_fuel.update_layout(height=300)
            st.plotly_chart(fig_fuel, use_container_width=True)

    with col2:
        # Speed distribution
        fig_speed = px.histogram(
            truck_df,
            x='event_speed',
            title='🏃 Speed Distribution',
            labels={'event_speed': 'Speed (km/h)', 'count': 'Frequency'}
        )
        fig_speed.update_layout(height=300)
        st.plotly_chart(fig_speed, use_container_width=True)

    # Data table
    st.subheader("📋 Filtered Data Summary")

    # Select key columns for display
    display_columns = [
        'event_occurred_at_parsed', 'event_latitude', 'event_longitude',
        'event_fuel_level', 'event_speed', 'event_engine_speed',
        'event_mileage', 'perf_drivers'
    ]

    available_columns = [col for col in display_columns if col in truck_df.columns]

    if available_columns:
        st.dataframe(
            truck_df[available_columns].head(100),  # Limit to 100 rows for performance
            use_container_width=True
        )

        if len(truck_df) > 100:
            st.info(f"📝 Showing first 100 rows of {len(truck_df)} total records")

    # Export functionality
    st.subheader("💾 Export Data")

    if st.button("📥 Download Filtered Data as CSV"):
        csv = truck_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{selected_truck}_performance_data.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()