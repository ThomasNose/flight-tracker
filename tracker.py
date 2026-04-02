import requests
import os
from dotenv import load_dotenv

load_dotenv()

key = os.getenv("API_KEY")

endpoints = ["flights", "routes", "airports", "airlines", "aircraft_types", "taxes", "cities"
             "countries", "timetable", "flightsFuture"]
for endpoint in endpoints:
    data = requests.get(f"https://api.aviationstack.com/v1/{endpoint}?access_key={key}")
    with open(f"flights/{endpoint}", "w") as f:
        f.write(data.text)
        f.close()