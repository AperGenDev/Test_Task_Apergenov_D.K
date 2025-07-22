import pandas as pd
import requests 
from sqlalchemy import create_engine
from datetime import datetime

def process_data(data):
    df_hourly = pd.DataFrame(data["hourly"])
    df_hourly['time'] = pd.to_datetime(df_hourly['time'], unit='s')

    df_daily = pd.DataFrame(data['daily'])
    df_daily['sunset'] = pd.to_datetime(df_daily['sunset'], unit='s')
    df_daily['sunrise'] = pd.to_datetime(df_daily['sunrise'], unit='s')
    df_daily['time'] = pd.to_datetime(df_daily['time'], unit='s')


    df_temp = pd.DataFrame({ #Основной дата-фрейм с преобразованными единицами измерения
        'time': df_hourly['time'],
        'avg_temperature_2m_24h': (df_hourly['temperature_2m'] - 32) * 5/9,  
        'avg_relative_humidity_2m_24h': (df_hourly["relative_humidity_2m"]),               	
        'avg_dew_point_2m_24h': (df_hourly['dew_point_2m'] - 32) * 5/9,
        'avg_apparent_temperature_24h': (df_hourly['apparent_temperature'] - 32) * 5/9,
        'avg_temperature_80m_24h': (df_hourly['temperature_80m'] - 32) * 5/9,
        'avg_temperature_120m_24h': (df_hourly['temperature_120m'] - 32) * 5/9,
        'avg_wind_speed_10m_24h': df_hourly['wind_speed_10m'] * 0.51444444444444,
        'avg_wind_speed_80m_24h': df_hourly['wind_speed_80m'] * 0.51444444444444,
        'avg_visibility_24h': df_hourly['visibility'] * 0.3048,
        'total_rain_24h': df_hourly['rain'] * 25.4,
        'total_showers_24h': df_hourly['showers'] * 25.4,
        'total_snowfall_24h': df_hourly['snowfall'] * 25.4,
        'wind_speed_10m_m_per_s': df_hourly['wind_speed_10m'] * 0.51444444444444,
        'wind_speed_80m_m_per_s': df_hourly['wind_speed_80m'] * 0.51444444444444,
        'temperature_2m_celsius': (df_hourly['temperature_2m'] - 32) * 5/9,
        'apparent_temperature_celsius': (df_hourly['apparent_temperature'] - 32) * 5/9,
        'temperature_80m_celsius': (df_hourly['temperature_80m'] - 32) * 5/9,
        'temperature_120m_celsius': (df_hourly['temperature_120m'] - 32) * 5/9,
        'soil_temperature_0cm_celsius': (df_hourly['soil_temperature_0cm'] - 32) * 5/9,
        'soil_temperature_6cm_celsius': (df_hourly['soil_temperature_6cm'] - 32) * 5/9,
        'rain_mm': df_hourly['rain'] * 25.4,
        'showers_mm': df_hourly['showers'] * 25.4,
        'snowfall_mm': df_hourly['snowfall'] * 25.4,
        'daylight_hours' : (df_daily['sunset'] - df_daily['sunrise']).dt.total_seconds() / 3600,
        'sunset_iso': df_daily['sunset'].dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'sunrise_iso': df_daily['sunrise'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    })

    def calc_daylight_mean(row, feature='temperature_2m'):   # Функция для расчета дневных показателей(СРЕДНЕЕ ЗНАЧЕНИЕ)
        mask = ((df_hourly['time'].dt.date == row['time'].date()) & (df_hourly['time'] >= row['sunrise']) & (df_hourly['time'] <= row['sunset']))
        return df_hourly.loc[mask, feature].mean()
    
    def calc_daylight_sum(row, feature='temperature_2m'):     # Тоже функция для расчета дневных показателей(СУММА)
        mask = ((df_hourly['time'].dt.date == row['time'].date()) & (df_hourly['time'] >= row['sunrise']) & (df_hourly['time'] <= row['sunset']))
        return df_hourly.loc[mask, feature].sum()

    # добавление дневных показателей
    df_daily['avg_temperature_2m_daylight'] = df_daily.apply(lambda x: (calc_daylight_mean(x, feature='temperature_2m') -32) * 5/9, axis=1)
    df_daily['avg_relative_humidity_2m_daylight'] = df_daily.apply(lambda x: calc_daylight_mean(x, feature='relative_humidity_2m'), axis=1)
    df_daily['avg_dew_point_2m_daylight'] = df_daily.apply(lambda x: (calc_daylight_mean(x, feature='dew_point_2m') - 32) * 5/9, axis=1)
    df_daily['avg_apparent_temperature_daylight'] = df_daily.apply(lambda x: (calc_daylight_mean(x, feature='apparent_temperature') - 32) * 5/9, axis=1)
    df_daily['avg_temperature_80m_daylight'] = df_daily.apply(lambda x: (calc_daylight_mean(x, feature='temperature_80m') - 32) * 5/9, axis=1)
    df_daily['avg_temperature_120m_daylight'] = df_daily.apply(lambda x: (calc_daylight_mean(x, feature='temperature_120m') - 32) * 5/9, axis=1)
    df_daily['avg_wind_speed_10m_daylight'] = df_daily.apply(lambda x: calc_daylight_mean(x, feature='wind_speed_10m') * 0.51444444444444, axis=1)
    df_daily['avg_wind_speed_80m_daylight'] = df_daily.apply(lambda x: calc_daylight_mean(x, feature='wind_speed_80m') * 0.51444444444444, axis=1)
    df_daily['avg_visibility_daylight'] = df_daily.apply(lambda x: calc_daylight_mean(x, feature='visibility') * 0.3048, axis=1)
    df_daily['total_rain_daylight'] = df_daily.apply(lambda x: calc_daylight_sum(x, feature='rain') * 25.4, axis=1)
    df_daily['total_showers_daylight'] = df_daily.apply(lambda x: calc_daylight_sum(x, feature='showers') * 25.4, axis=1)
    df_daily['total_snowfall_daylight'] = df_daily.apply(lambda x: calc_daylight_sum(x, feature='snowfall') * 25.4, axis=1)

    day_mean = df_temp.resample('D', on='time').agg({  #Агрегация по дням
        'avg_temperature_2m_24h': 'mean',
        'avg_relative_humidity_2m_24h': 'mean',
        'avg_dew_point_2m_24h': 'mean', 
        'avg_apparent_temperature_24h': 'mean',
        'avg_temperature_80m_24h': 'mean',
        'avg_temperature_120m_24h': 'mean',
        'avg_wind_speed_10m_24h': 'mean',
        'avg_wind_speed_80m_24h': 'mean',
        'avg_visibility_24h': 'mean',
        'total_rain_24h': 'sum',
        'total_showers_24h': 'sum',
        'total_snowfall_24h': 'sum',
    })

    day_mean_reset = day_mean.reset_index()
    df_daily['time_date'] = pd.to_datetime(df_daily['time'].dt.date)
    final_df = day_mean_reset.merge(
        df_daily[['time_date', 'avg_temperature_2m_daylight', 'avg_relative_humidity_2m_daylight', 'avg_dew_point_2m_daylight', 'avg_apparent_temperature_daylight',
                 'avg_temperature_80m_daylight', 'avg_temperature_120m_daylight', 'avg_wind_speed_10m_daylight', 'avg_wind_speed_80m_daylight', 'avg_visibility_daylight', 'total_rain_daylight',
                 'total_showers_daylight', 'total_snowfall_daylight']],
        left_on=pd.to_datetime(day_mean_reset['time'].dt.date),
        right_on='time_date',
        how='left'
    )
    final_df.drop('time_date', axis=1, inplace=True)

    hourly_wind = df_temp[['time', 'wind_speed_10m_m_per_s', 'wind_speed_80m_m_per_s', 'temperature_2m_celsius', 'apparent_temperature_celsius', 'temperature_80m_celsius',
                          'temperature_120m_celsius', 'soil_temperature_0cm_celsius', 'soil_temperature_6cm_celsius', 'rain_mm', 'showers_mm', 'snowfall_mm', 'daylight_hours',
                          'sunset_iso', 'sunrise_iso']].copy()  

    final_df_with_hourly = pd.merge(
        final_df,
        hourly_wind,
        left_on=final_df['time'].dt.date,
        right_on=hourly_wind['time'].dt.date,
        how='left'
    )

    final_df_with_hourly.drop(['key_0', 'time_x', 'time_y'], axis=1, inplace=True)

    return final_df_with_hourly

def save_results(df, csv_path, db_url): #Функция сохранения данных в csv и базу данных
    df.to_csv(csv_path, index=False) 
    engine = create_engine(db_url) 
    df.to_sql('weather_data', engine, if_exists='append', index=False) 



def interval_date(start, end):   #Функция для запрашивания данных за определённый период 
    start = datetime.strptime(start, "%Y-%m-%d").date()
    end = datetime.strptime(end, "%Y-%m-%d").date()

    if start > end:
        raise ValueError("Дата начала не может быть позже даты окончания!!!")
            

    if((end - start).days > 365):
        raise ValueError("Диапазон дат не может превышать 1 год")
    
    url = 'https://api.open-meteo.com/v1/forecast'
    params={
        'latitude': 55.0344,
        'longitude': 82.9434,
        'daily': 'sunrise,sunset,daylight_duration',
        'hourly': 'temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall',
        'timezone': 'auto',
        'timeformat': 'unixtime',
        'wind_speed_unit': 'kn',
        'temperature_unit': 'fahrenheit',
        'precipitation_unit': 'inch',
        'start_date': start,
        'end_date': end
    }
    response = requests.get(url, params=params)
    return response.json()
    
if __name__ == "__main__":
    url = 'https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-05-16&end_date=2025-05-30'
    csv_path = 'D:/Weather_project/data/MyWeather.csv'
    db_url = 'postgresql://weather_user:weather_pass@localhost:5432/weather_db'
    print('Хотите ли вы задать временной интервал? Введите "Y" если да, введите иной символ если нет:')
    o = input()
    if o == 'Y':
        start = input('Введите начало временного отрезка:')        
        end = input('Введите конец временного отрезка:')
        data = interval_date(start, end)
        df = process_data(data)
        save_results(df, csv_path, db_url)
        print("Данные успешно сохранены")
    else:
        response = requests.get(url)
        data = response.json()
        df = process_data(data)
        save_results(df, csv_path, db_url)
        print("Данные успешно сохранены")
