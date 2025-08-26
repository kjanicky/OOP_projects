import requests

class WeatherFetcher:
    def __init__(self,url,params,type):
        self.url = url
        self.params = params
        self.type = type

    def fetch_data(self):
        try:
            response = requests.get(f"{self.url}{self.type}",params=self.params, timeout=500)
            if response.status_code == 200:
                print("Request went sucessfully")
                return response.json()
            elif response.status_code == 400:
                print("Bad request - Error Code 400")
            elif response.status_code == 404:
                print("Request Not found - Error COde 400")
            else:
                print(f"Unexpected error: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Network error {e}")

print(WeatherFetcher("https://api.open-meteo.com/v1/",{
	"latitude": 52.52,
    "longitude": 13.41,
    "start_date": "2025-08-20",
    "end_date": "2025-08-24",
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone": "Europe/Berlin"
},"forecast").fetch_data()) #just a validation print

