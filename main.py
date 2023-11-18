from enum import Enum

import streamlit as st
import pandas as pd
import requests
import xmltodict
import pymysql
import sqlalchemy


def get_initial_weather_dataframe(lat, long):
    forecast = requests.get(
        'https://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast?lat=' + lat + ';long=' + long).text
    xmlDict = xmltodict.parse(forecast)
    xmlDict_norm = pd.json_normalize(xmlDict['weatherdata']['product'], record_path='time')
    return pd.DataFrame.from_dict(xmlDict_norm)


def clean_weather_dataframe(df):
    df.drop(['@datatype', 'location.@altitude', 'location.@latitude', 'location.@longitude', 'location.temperature.@id',
             'location.windDirection.@id', 'location.windSpeed.@id', 'location.windGust.@id', 'location.pressure.@id',
             'location.cloudiness.@id', 'location.lowClouds.@id', 'location.lowClouds.@percent',
             'location.mediumClouds.@id', 'location.mediumClouds.@percent', 'location.highClouds.@id',
             'location.highClouds.@percent',
             'location.dewpointTemperature.@id', 'location.symbol.@number'], axis=1, errors='ignore', inplace=True)
    df = df.groupby('@from').first().reset_index()
    df.drop('@to', axis=1, errors='ignore', inplace=True)
    df.columns = ['date', 'temperature unit', 'temperature', 'wind direction degree', 'wind direction name',
                  'wind speed mps', 'wind speed beaufort', 'wind speed name', 'wind gust mps', 'global radiation value',
                  'global radiation unit', 'humidity value', 'humidity unit', 'pressure unit', 'pressure value',
                  'cloudiness percentage', 'dewpoint temperature unit', 'dewpoint temperature', 'precipitation unit',
                  'precipitation value', 'min. precipitation', 'max. precipitation', 'probability of precipitation',
                  'overall']

    df['temperature'] = df['temperature'].astype(float)
    df['wind direction degree'] = df['wind direction degree'].astype(float)
    df['wind speed mps'] = df['wind speed mps'].astype(float)
    df['wind speed beaufort'] = df['wind speed beaufort'].astype(float)
    df['wind gust mps'] = df['wind gust mps'].astype(float)
    df['global radiation value'] = df['global radiation value'].astype(float)
    df['humidity value'] = df['humidity value'].astype(float)
    df['pressure value'] = df['pressure value'].astype(float)
    df['dewpoint temperature'] = df['dewpoint temperature'].astype(float)
    df['precipitation value'] = df['precipitation value'].astype(float)
    df['min. precipitation'] = df['min. precipitation'].astype(float)
    df['max. precipitation'] = df['max. precipitation'].astype(float)
    df['probability of precipitation'] = df['probability of precipitation'].astype(float)

    df['date'] = pd.to_datetime(df['date'])

    df.sort_values(by=['date'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def calculate_forecast_data_per_day(df):
    x = df.groupby([df['date'].dt.date]).agg({'temperature': ['min', 'max', 'mean'],
                                              'wind speed mps': ['min', 'max', 'mean'],
                                              'precipitation value': ['sum'],
                                              'humidity value': ['min', 'max', 'mean'],
                                              'pressure value': ['min', 'max']})
    x.reset_index(inplace=True)
    x.drop(index=x.index[0], axis=0, inplace=True)
    x.columns = ['date', 'min. temperature', 'max. temperature', 'mean temperature', 'min. wind speed',
                 'max. wind speed',
                 'mean wind speed', 'total precipitation', 'min. humidity', 'max. humidity', 'mean humidity',
                 'min. pressure', 'max. pressure']
    return x


def get_lat_long(city):
    if city == 'Cork':
        return '51.8930755', '-8.5008956'
    elif city == 'Dublin':
        return '53.3454191', '-6.2684563'
    elif city == 'Galway':
        return '53.271528', '-9.055541'
    else:
        return -1, -1


def get_fine_grained_dataframe(df):
    return df.drop(df[df['date'].dt.date > (pd.Timestamp.now() + pd.Timedelta(days=3)).date()].index)


def get_last_update_time_of_database(city):
    table_name = 'weather_forecast_' + city.lower() + '_' + df_type.CLEANED.name.lower()
    database_connection = pymysql.connect(
        host='XXX.eu-west-1.rds.amazonaws.com',
        user='XXX', password='XXX', db='ebdb', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with database_connection.cursor() as cursor:
            sql = "SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA='ebdb' AND TABLE_NAME='" + table_name + "'"
            cursor.execute(sql)
            result = cursor.fetchone()
            return result['UPDATE_TIME']
    except:
        print("Error: Could not get last update time of database for " + city)
    finally:
        database_connection.close()


def store_dataframe_to_database(df, city, type):
    table_name = 'weather_forecast_' + city.lower() + '_' + type.name.lower()
    engine = sqlalchemy.create_engine(
        'mysql+pymysql://XXX:XXX@XXX.eu-west-1.rds.amazonaws.com:3306/ebdb')
    database_connection = engine.connect()
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    except:
        print("Error: Could not store dataframe to database.")
    finally:
        database_connection.close()


def get_dataframe_from_database(city, type):
    table_name = 'weather_forecast_' + city.lower() + '_' + type.name.lower()
    engine = sqlalchemy.create_engine(
        'mysql+pymysql://XXX:XXX@XXX.eu-west-1.rds.amazonaws.com:3306/ebdb')
    database_connection = engine.connect()
    try:
        return pd.read_sql_table(table_name, engine)
    except:
        print("Error: Could not get dataframe from database.")
    finally:
        database_connection.close()


def configure_page():
    st.set_page_config(
        page_title="Weather Forecast for Ireland",
        page_icon="🌤️",
        layout="centered",
        initial_sidebar_state="expanded",
        menu_items={
            'About': "Weather Forecast for Ireland by Nicolas Weber as part of the CS3204 Cloud Infrastructure "
                     "and Services module at UCC."
        }
    )
    st.title("Weather Forecast for Ireland")


def get_dataframes(city):
    # if forecast stored in AWS database is less than 1 hour old, use it. Otherwise, use fresh data from Met Éireann
    # API and update the AWS database.
    if (pd.Timestamp.now() - pd.Timestamp(get_last_update_time_of_database(city))) < pd.Timedelta(hours=1):
        df = get_dataframe_from_database(city, df_type.CLEANED)
        df_only_fine_grained = get_dataframe_from_database(city, df_type.FINE_GRAINED)
        df_grouped_by_day = get_dataframe_from_database(city, df_type.GROUPED_BY_DAY)
    else:
        lat, long = get_lat_long(city)
        df_initial = get_initial_weather_dataframe(lat, long)
        df = clean_weather_dataframe(df_initial)
        df_only_fine_grained = get_fine_grained_dataframe(df)
        df_grouped_by_day = calculate_forecast_data_per_day(df)
        store_dataframe_to_database(df, city, df_type.CLEANED)
        store_dataframe_to_database(df_only_fine_grained, city, df_type.FINE_GRAINED)
        store_dataframe_to_database(df_grouped_by_day, city, df_type.GROUPED_BY_DAY)
    return df, df_only_fine_grained, df_grouped_by_day


def generate_header(city, df):
    st.subheader("Current Weather in " + city)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label="Temperature", value=df['temperature'].iloc[1].astype(str) + "°C")
    col2.metric(label="Wind Speed", value=df['wind speed mps'].iloc[1].astype(str) + "mph")
    col3.metric(label="Humidity", value=df['humidity value'].iloc[1].astype(str) + "%")
    col4.metric(label="Air Pressure", value=df['pressure value'].iloc[1].astype(str) + "hPa")


def generate_main_content(city, df_only_fine_grained, df_grouped_by_day):
    st.divider()

    st.subheader("Temperature in " + city)
    st.write("Let's have a look at a fine-grained temperature chart:")
    st.line_chart(df_only_fine_grained[['date', 'temperature']], x='date', y='temperature')
    st.write("A chart of the minimum, mean and maximum temperatures forecasted for the upcoming days:")
    st.line_chart(df_grouped_by_day[['date', 'min. temperature', 'mean temperature', 'max. temperature']], x='date',
                  y=['min. temperature', 'mean temperature', 'max. temperature'],
                  color=['#FF7F7F', '#90EE90', '#ADD8E6'])

    st.subheader("Precipitation in " + city)
    st.write("Now, let's have a look at a fine-grained precipitation chart:")
    st.bar_chart(df_only_fine_grained[['date', 'precipitation value']], x='date', y='precipitation value')
    st.write("You might be interested in the precipitation per day for a longer period of time. Including today, " +
             df_grouped_by_day.loc[df_grouped_by_day['total precipitation'] > 0, 'total precipitation'].count().astype(
                 str) +
             " out of the next " + df_grouped_by_day['total precipitation'].count().astype(
        str) + " days will likely have rain:")
    st.bar_chart(df_grouped_by_day[['date', 'total precipitation']], x='date', y='total precipitation')

    st.subheader("Humidity in " + city)
    st.write("Maybe the fine-grained humidity chart is also interesting:")
    st.line_chart(df_only_fine_grained[['date', 'humidity value']], x='date', y='humidity value')
    st.write("Let's have a look at the minimum, mean and maximum humidity forecasted for the upcoming days:")
    st.line_chart(df_grouped_by_day[['date', 'min. humidity', 'mean humidity', 'max. humidity']], x='date',
                  y=['min. humidity', 'mean humidity', 'max. humidity'],
                  color=['#FF7F7F', '#90EE90', '#ADD8E6'])

    st.subheader("Wind Speed in " + city)
    st.write("The fine-grained wind speed chart might also be interesting:")
    st.line_chart(df_only_fine_grained[['date', 'wind speed mps']], x='date', y='wind speed mps')
    st.write("Let's have a look at the minimum, mean and maximum wind speed forecasted for the upcoming days:")
    st.line_chart(df_grouped_by_day[['date', 'min. wind speed', 'mean wind speed', 'max. wind speed']], x='date',
                  y=['min. wind speed', 'mean wind speed', 'max. wind speed'],
                  color=['#FF7F7F', '#90EE90', '#ADD8E6'])


def generate_footer(df, df_only_fine_grained, df_grouped_by_day):
    st.divider()
    st.subheader("Raw Data")
    with st.expander("See calculated forecast data per day"):
        st.dataframe(df_grouped_by_day, hide_index=True)

    with st.expander("See raw data provided by Met Éireann weather forecast API"):
        st.write("You can have a look at the cleaned raw data provided by Met Éireann:")
        columns_shown = ['date', 'temperature', 'wind direction degree',
                         'wind direction name', 'wind speed mps',
                         'wind speed beaufort', 'wind speed name',
                         'wind gust mps', 'global radiation value',
                         'humidity value', 'pressure value',
                         'cloudiness percentage', 'dewpoint temperature',
                         'precipitation value', 'min. precipitation',
                         'max. precipitation', 'probability of precipitation',
                         'overall']
        st.dataframe(df[columns_shown], hide_index=True)
        st.write("See only the finer-grained data:")
        st.dataframe(df_only_fine_grained[columns_shown], hide_index=True)


configure_page()
city = st.selectbox('Select City', ('Cork', 'Dublin', 'Galway'))
df_type = Enum('Type', ['CLEANED', 'FINE_GRAINED', 'GROUPED_BY_DAY'])
df, df_only_fine_grained, df_grouped_by_day = get_dataframes(city)
generate_header(city, df)
generate_main_content(city, df_only_fine_grained, df_grouped_by_day)
generate_footer(df, df_only_fine_grained, df_grouped_by_day)
