from datetime import datetime

from debugpy.adapter.components import missing


class WeatherTransformer():
    def __init__(self,data):
        self.data = data

    def validate_keys(self): #validation of expected keys
        expected_keys = {'date','temperature_2m_max','temperature_2m_min','precipitation_sum','latitude','longitude','timezone'}
        keys_in_data = set(self.data.keys())
        missing_keys = expected_keys - keys_in_data
        if missing_keys :
            raise ValueError(f"Missing keys: {missing_keys}")

        return missing_keys

    def validate_daily(self): # method to validate key daily data before parsing
        if 'daily' not in self.data:
            raise ValueError("Key 'daily' is not present in the data")
        missing_keys = []

        for k, v in self.data['daily'].items():
            if not v or any(item is None for item in v):
                missing_keys.append(k)

        if missing_keys:
            raise ValueError(f"The following keys in 'daily' are missing or contain invalid data: {missing_keys}")




    def find_empty_values(self):
        empty_keys = []
        for k, v in self.data.items():
            if v is None:
                empty_keys.append(k)

        if empty_keys:
            raise ValueError(f"Values in those keys are missing: {empty_keys}")

    def json_parser(self): # classic parser with zip
        json_parsed = []
        daily = self.data['daily']
        lat = self.data.get('latitude')
        lon = self.data.get('longitude')
        timezone = self.data.get('timezone')
        for date, temp_max, temp_min, precipitation in zip(
                daily['time'],
                daily['temperature_2m_max'],
                daily['temperature_2m_min'],
                daily['precipitation_sum']
        ):
            json_parsed.append({
                "date":date,
                "temperature_2m_max":temp_max,
                "temperature_2m_min":temp_min,
                "precipitation_sum":precipitation,
                "latitude":lat,
                "longitude":lon,
                "timezone":timezone,
            })

        return json_parsed

    def get_last_date(self):
        parsed_data = self.json_parser()
        max_date = max(datetime.strptime(entry['date'],"%Y-%m-%d") for entry in parsed_data)
        return max_date



#print(WeatherTransformer({'latitude': 52.52, 'longitude': 13.419998, 'generationtime_ms': 0.3031492233276367, 'utc_offset_seconds': 7200, 'timezone': 'Europe/Berlin', 'timezone_abbreviation': 'GMT+2', 'elevation': 38.0, 'daily_units': {'time': 'iso8601', 'temperature_2m_max': '°C', 'temperature_2m_min': '°C', 'precipitation_sum': 'mm'}, 'daily': {'time': ['2025-08-20', '2025-08-21', '2025-08-22', '2025-08-23', '2025-08-24'], 'temperature_2m_max': [24.0, 23.9, 19.1, 16.6, 19.5], 'temperature_2m_min': [15.7, 11.4, 11.4, 12.8, 9.5], 'precipitation_sum': [0.0, 0.0, 0.0, 0.2, 0.0]}}).json_parser())
print(WeatherTransformer({'latitude': 52.52, 'longitude': 13.419998, 'generationtime_ms': 0.3031492233276367, 'utc_offset_seconds': 7200, 'timezone': 'Europe/Berlin', 'timezone_abbreviation': 'GMT+2', 'elevation': 38.0, 'daily_units': {'time': 'iso8601', 'temperature_2m_max': '°C', 'temperature_2m_min': '°C', 'precipitation_sum': 'mm'}, 'daily': {'time': ['2025-08-20', '2025-08-21', '2025-08-22', '2025-08-23', '2025-08-24'], 'temperature_2m_max': [24.0, 23.9, 19.1, 16.6, 19.5], 'temperature_2m_min': [15.7, 11.4, 11.4, 12.8, 9.5], 'precipitation_sum': [0.0, 0.0, 0.0, 0.2, 0.0]}}).json_parser())
test_data_missing_daily_key = {
    'latitude': 52.52,
    'longitude': 13.42,
    'generationtime_ms': 0.3,
    'utc_offset_seconds': 7200,
    'timezone': 'Europe/Berlin',
    'timezone_abbreviation': 'GMT+2',
    'elevation': 38.0,
    'daily_units': {
        'time': 'iso8601',
        'temperature_2m_max': '°C',
        'temperature_2m_min': '°C',
        'precipitation_sum': 'mm'
    }}
WeatherTransformer(test_data_missing_daily_key).validate_daily()