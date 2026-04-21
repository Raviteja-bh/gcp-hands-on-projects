import requests
import time
from datetime import datetime, timedelta


# safe_api_call — a helper function to handle retries and rate limits for all API calls

def safe_api_call(url, params=None, headers=None, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                print(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)

            else:
                print(f"Error {response.status_code} from {url}")
                return None

        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt + 1}")

        except requests.exceptions.ConnectionError:
            print(f"Connection error on attempt {attempt + 1}")

    print("All retries failed")
    return None


#USGS — now uses safe_api_call instead of requests.get, and handles failure gracefully
def fetch_earthquakes(min_magnitude=2.5, days_back=1):
    data = safe_api_call(                          # ← wrapped here
        url="https://earthquake.usgs.gov/fdsnws/event/1/query",
        params={
            "format": "geojson",
            "starttime": (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d"),
            "endtime": datetime.utcnow().strftime("%Y-%m-%d"),
            "minmagnitude": min_magnitude,
            "orderby": "time"
        }
    )

    if data is None:                               # ← handle failure
        print("USGS fetch failed — returning empty list")
        return []

    earthquakes = data["features"]
    print(f"USGS: Found {len(earthquakes)} earthquakes")
    return earthquakes


#NOMAA — now uses safe_api_call instead of requests.get, and handles failure gracefully

def fetch_noaa_alerts(state="FL"):
    data = safe_api_call(                          # ← wrapped here
        url="https://api.weather.gov/alerts/active",
        params={
            "status": "actual",
            "area": state
        },
        headers={
            "User-Agent": "cat-risk-pipeline (test@email.com)",
            "Accept": "application/json"
        }
    )

    if data is None:                               # ← handle failure
        print("NOAA fetch failed — returning empty list")
        return []

    alerts = data["features"]
    print(f"NOAA: Found {len(alerts)} active alerts in {state}")
    return alerts


# FEMA — now uses safe_api_call instead of requests.get, and handles failure gracefully

def fetch_fema_disasters(days_back=30):
    data = safe_api_call(                          # ← wrapped here
        url="https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
        params={
            "$top": 5,
            "$orderby": "declarationDate desc",
            "$filter": f"declarationDate gt '{(datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')}'"
        },
        headers={
            "Accept": "application/json",
            "User-Agent": "cat-risk-pipeline (test@email.com)"
        }
    )

    if data is None:                               # ← handle failure
        print("FEMA fetch failed — returning empty list")
        return []

    disasters = data["DisasterDeclarationsSummaries"]
    print(f"FEMA: Found {len(disasters)} disaster declarations")
    return disasters


# Run all 3 tests together
if __name__ == "__main__":
    print("=== USGS Earthquakes ===")
    earthquakes = fetch_earthquakes(min_magnitude=4.0)
    if earthquakes:
        print(f"Latest: {earthquakes[0]['properties']['title']}")

    print("\n=== NOAA Alerts ===")
    alerts = fetch_noaa_alerts(state="FL")
    if alerts:
        print(f"Latest: {alerts[0]['properties']['headline']}")

    print("\n=== FEMA Disasters ===")
    disasters = fetch_fema_disasters(days_back=30)
    if disasters:
        print(f"Latest: {disasters[0]['declarationTitle']} - {disasters[0]['state']}")