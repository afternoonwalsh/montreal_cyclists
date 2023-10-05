import pandas as pd
import os
from datetime import datetime
from datetime import date
from unidecode import unidecode
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Calculte the absolute and relative change of the metrics week on week
def generate_change_values(new_value, original_value, metric='other'):    
    if pd.isna(original_value) == True:
        change_value = None
        change_value_pct = None
    elif original_value == 0:
        change_value = new_value - original_value
        if metric == 'passage_count':
            change_value_pct = (new_value - 1)/1
        else:
            change_value_pct = 1
    elif metric == 'mean_temp':
        change_value = new_value - original_value
        change_value_pct = change_value/original_value
        if (change_value > 0 and change_value_pct < 0) or (change_value < 0 and change_value_pct > 0):
            change_value_pct = change_value_pct * -1
    else:
        change_value = new_value - original_value
        change_value_pct = change_value/original_value
        
    return change_value, change_value_pct

# Create columns to add to dataframe for change and relative change
def generate_change_columns(row):
    passage_count_change, passage_count_change_pct = (generate_change_values(row['passage_count'], row['passage_count_last_week'],'passage_count'))
    mean_temp_change, mean_temp_change_pct = (generate_change_values(row['mean_temp'], row['mean_temp_last_week'], 'mean_temp'))
    total_rain_change, total_rain_change_pct = (generate_change_values(row['total_rain'], row['total_rain_last_week']))
    snow_on_ground_change, snow_on_ground_pct = (generate_change_values(row['snow_on_ground'], row['snow_on_ground_last_week']))
    return pd.Series({'passage_count_change': passage_count_change,
                     'passage_count_change_pct': passage_count_change_pct,
                     'mean_temp_change': mean_temp_change, 
                     'mean_temp_change_pct': mean_temp_change_pct,
                     'total_rain_change': total_rain_change, 
                     'total_rain_change_pct': total_rain_change_pct,
                     'snow_on_ground_change': snow_on_ground_change,
                     'snow_on_ground_pct': snow_on_ground_pct
                     })

@transformer
def transform(dataframe_list, *args, **kwargs):

    passage_df = dataframe_list[0]
    weather_df = dataframe_list[1]
    date_df = dataframe_list[2]
    sensor_df = dataframe_list[3]

    passage_grp_df = passage_df.groupby(['date','sensor_id'])['passage_count'].sum().reset_index()
    analytics_df = passage_grp_df \
              .merge(sensor_df, on='sensor_id', how='left') \
              .merge(weather_df, on='date', how='left')
    analytics_df = analytics_df[['date', 'sensor_id', 'passage_count', 'latitude', 'longitude',
       'name', 'weather_id', 'min_temp', 'mean_temp', 'max_temp',
       'total_rain', 'total_precip',
       'snow_on_ground', 'mean_temp_band',
       'mean_temp_start', 'total_rain_band', 'total_rain_start',
       'snow_on_ground_band', 'snow_on_ground_start']]

    # Calculate the passage count, mean temp, rain, and snow of date-7
    def generate_last_week_comparison(row):
        current_sensor = row['sensor_id']
        date_last_week = pd.to_datetime(row['date'] - pd.DateOffset(days=7))
        values_last_week = (
            analytics_df[(pd.to_datetime(analytics_df['date']) == date_last_week) 
                        & (analytics_df['sensor_id'] == current_sensor)][['passage_count','mean_temp','total_rain','snow_on_ground']].values)
        
        if len(values_last_week) > 0:
            passage_count_last_week = values_last_week[0][0]
            mean_temp_last_week = values_last_week[0][1]
            total_rain_last_week = values_last_week[0][2]
            snow_on_ground_last_week = values_last_week[0][3]
        else:
            (passage_count_last_week, mean_temp_last_week, 
            total_rain_last_week, snow_on_ground_last_week) = (None, None, None, None)
        
        return pd.Series({'date_last_week': date_last_week,
                        'passage_count_last_week':passage_count_last_week,
                        'mean_temp_last_week':mean_temp_last_week, 
                        'total_rain_last_week': total_rain_last_week,
                        'snow_on_ground_last_week': snow_on_ground_last_week})

    def generate_ratio(row, metric):
        if row[metric] == 0:
            return None
        else:
            return row['passage_count_change']/row[metric]


    new_weather_columns = analytics_df.apply(generate_last_week_comparison, axis=1)
    analytics_df = pd.concat([analytics_df, new_weather_columns], axis=1)

    new_change_columns = analytics_df.apply(generate_change_columns, axis=1)
    analytics_df = pd.concat([analytics_df, new_change_columns], axis=1)

    analytics_df['temp_passage_change_ratio'] = analytics_df.apply(generate_ratio,metric='mean_temp_change', axis=1)
    analytics_df['rain_passage_change_ratio'] = analytics_df.apply(generate_ratio,metric='total_rain_change', axis=1)
    analytics_df['snow_passage_change_ratio'] = analytics_df.apply(generate_ratio,metric='snow_on_ground', axis=1)

    return analytics_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
