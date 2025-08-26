from datetime import datetime

class WeatherTransformer():
    def __init__(self,data):
        self.data = data

    def json_parser(self):
        json_parsed = []
        dates = self.data['daily']['time'] # could use zip instead that guarantess same list lenght but this is my version :)
        for i in range(len(dates)):
            json_parsed.append({
             "date": self.data['daily']['time'][i],
                "temperature_2m_max": self.data['daily']['temperature_2m_max'][i],
                "temperature_2m_min": self.data['daily']['temperature_2m_min'][i],
                "precipitation_sum": self.data['daily']['precipitation_sum'][i]
            })

        return json_parsed

    def get_last_date(self):
        parsed_data = self.json_parser()
        max_date = max(datetime.strptime(entry['date'],"%Y-%m-%d") for entry in parsed_data)
        return max_date



#print(WeatherTransformer({'latitude': 52.52, 'longitude': 13.419998, 'generationtime_ms': 0.3031492233276367, 'utc_offset_seconds': 7200, 'timezone': 'Europe/Berlin', 'timezone_abbreviation': 'GMT+2', 'elevation': 38.0, 'daily_units': {'time': 'iso8601', 'temperature_2m_max': '°C', 'temperature_2m_min': '°C', 'precipitation_sum': 'mm'}, 'daily': {'time': ['2025-08-20', '2025-08-21', '2025-08-22', '2025-08-23', '2025-08-24'], 'temperature_2m_max': [24.0, 23.9, 19.1, 16.6, 19.5], 'temperature_2m_min': [15.7, 11.4, 11.4, 12.8, 9.5], 'precipitation_sum': [0.0, 0.0, 0.0, 0.2, 0.0]}}).get_last_date())
