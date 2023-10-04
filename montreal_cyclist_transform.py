import pandas as pd
from datetime import datetime
from unidecode import unidecode
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Set global variables for time ranges (version 1)
morning_range = range(6, 12)  # 6 AM to 11:59 AM
afternoon_range = range(12, 18)  # 12 PM to 5:59 PM
evening_range = range(18, 24)  # 6 PM to 11:59 PM

# Set global variables for time ranges (version 2)
early_morning_range = range(4, 7)  # 4 AM to 6:59 AM
late_night_range = range(0, 4)  # 12 AM to 3:59 AM

# Function to categorize timestamp into parts of day (version 1)
def categorize_time(timestamp):
    hour = timestamp.hour
    if hour in morning_range:
        return "1 - Morning"
    elif hour in afternoon_range:
        return "2 - Afternoon"
    elif hour in evening_range:
        return "3 - Evening"
    else:
        return "4 - Late Night"

# Function to categorize timestamp into parts of day (version 2)
def categorize_time_off_hours(timestamp):
    hour = timestamp.hour
    if hour in early_morning_range:
        return "Early Morning"
    elif hour in late_night_range:
        return "Late Night"
    else:
        return "None"

# Define a custom function to convert integers to strings with leading zeros
def format_with_leading_zeros(number):
    return f"{number:02d}"  # 02d means 2 digits with leading zeros

@transformer
def transform(sensor_df, passage_df, weather_df, *args, **kwargs):
    # Create a date dataframe
    start_date = '2022-01-01'
    end_date = '2022-12-31'
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_df = pd.DataFrame({'timestamp': date_range})
    date_df['date'] = date_df['timestamp'].dt.date
    date_df['day_of_week'] = date_df['timestamp'].dt.day_name()
    date_df['month'] = date_df['timestamp'].dt.month_name()
    date_df['month_num'] = date_df['timestamp'].dt.month
    date_df['month_label'] = date_df['month_num'].apply(format_with_leading_zeros) + ' - ' + date_df['month']
    date_df['month_label']
    #date_df['month_label'] = date_df['month_num'].astype(str) + ' - ' + date_df['month']
    date_df['quarter'] = date_df['timestamp'].dt.quarter
    date_df['year'] = date_df['timestamp'].dt.year
    date_df.head()
    
    # Passage
    passage_df.rename(columns={
        'Date':'timestamp',
    },inplace=True)
    passage_df = passage_df.melt(id_vars='timestamp', var_name='sensor', value_name='passage_count')
    passage_df['passage_id'] = passage_df.index
    passage_df['sensor_id'] = passage_df['sensor'].str.replace('compteur_','')
    passage_df['passage_count'].fillna(0,inplace=True)
    passage_df['timestamp'] = pd.to_datetime(passage_df['timestamp'])
    passage_df['part_of_day'] = passage_df['timestamp'].apply(categorize_time)
    passage_df['off_hours_category'] = passage_df['timestamp'].apply(categorize_time_off_hours)
    passage_df['date'] = passage_df['timestamp'].dt.date
    passage_df = passage_df[['passage_id','sensor_id', 'date','timestamp', 'part_of_day', 'off_hours_category', 'passage_count']]

    # Sensor
    sensor_df = sensor_df[sensor_df['Statut']=='Actif']
    sensor_df = sensor_df[sensor_df['Nom']!='REV St Denis/Castelnau NB'] # Fix duplicate record
    sensor_df['name'] = sensor_df['Nom'].apply(unidecode)
    sensor_df['sensor_id'] = sensor_df['ID'].astype(str)
    sensor_df.rename(columns={
        #'ID':'sensor_id',
        #'Nom':'name',
        'Statut':'status',
        'Latitude':'latitude',
        'Longitude':'longitude'
    },inplace=True)
    sensor_df = sensor_df[['sensor_id','name','latitude','longitude']]

    # Weather
    weather_df['date'] = pd.to_datetime(weather_df['Date/Time']).dt.date
    weather_sensor_long = weather_df['ï»¿Longitude (x)'][0]
    weather_sensor_lat = weather_df['Latitude (y)'][0]
    weather_df['weather_id'] = weather_df.index
    weather_df['snow_on_ground'] = weather_df['Snow on Grnd (cm)'].fillna(0)
    weather_df['total_rain_flag'] = [True if value is not None and value != 0 else False for value in weather_df['Total Rain (mm)']]
    weather_df['snow_on_ground_flag'] = [True if value is not None and value != 0 else False for value in weather_df['snow_on_ground']]

    weather_df.rename(columns={
        'Min Temp (Â°C)':'min_temp',
        'Mean Temp (Â°C)':'mean_temp',
        'Max Temp (Â°C)':'max_temp',
        'Total Rain (mm)':'total_rain',
        #'Total Rain Flag':'total_rain_flag',
        'Total Precip (mm)':'total_precip',
        'Total Precip Flag':'total_precip_flag',
        #'Snow on Grnd (cm)':'snow_on_ground',
        #'Snow on Grnd Flag':'snow_on_ground_flag',
    },inplace=True)
    # Temp bands
    bin_edges = [-30, -25, -20, -15, -10, -5, 0, 5, 10, 15, 20, 25, 30]  # Bins of width 5
    bin_edges_start = [-30, -25, -20, -15, -10, -5, 0, 5, 10, 15, 20, 25]
    bin_labels = ['-30 to 25', '-25 to -20', '-20 to -15', '-15 to -10', 
                '-10 to -5', '-5 to 0', '0 to 5', '5 to 10', '10 to 15', '15 to 20', '20 to 25', '25 to 30']
    weather_df['mean_temp_band'] = pd.cut(weather_df['mean_temp'], bins=bin_edges, labels=bin_labels)
    weather_df['mean_temp_start'] = pd.cut(weather_df['mean_temp'], bins=bin_edges, labels=bin_edges_start)
    
    # Rain bands
    bin_edges = [-1, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]  # Bins of width 5
    bin_edges_start = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    bin_labels = ['0', '0 to 5', '5 to 10', '10 to 15', '15 to 20', 
                '20 to 25', '25 to 30', '30 to 35', '35 to 40', '40 to 45', '45 to 50']
    weather_df['total_rain_band'] = pd.cut(weather_df['total_rain'], bins=bin_edges, labels=bin_labels)
    weather_df['total_rain_start'] = pd.cut(weather_df['total_rain'], bins=bin_edges, labels=bin_edges_start)

    # Snow bands
    # Define the bin edges
    bin_edges = [-1, 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30]  # Bins of width 5
    bin_edges_start = [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30]
    bin_labels = ['0','0 to 3', '3 to 6', '6 to 9', '9 to 12', 
                '12 to 15', '15 to 18', '18 to 21', '21 to 24', '24 to 27', '27 to 30']
    weather_df['snow_on_ground_band'] = pd.cut(weather_df['snow_on_ground'], bins=bin_edges, labels=bin_labels)
    weather_df['snow_on_ground_start'] = pd.cut(weather_df['snow_on_ground'], bins=bin_edges, labels=bin_edges_start)

    weather_df = weather_df[['weather_id','date','min_temp','mean_temp','max_temp',
           'total_rain','total_rain_flag','total_precip',
           'total_precip_flag','snow_on_ground','snow_on_ground_flag',
           'mean_temp_band', 'mean_temp_start',
           'total_rain_band', 'total_rain_start',
           'snow_on_ground_band', 'snow_on_ground_start']]

    return [passage_df, weather_df, date_df, sensor_df]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
