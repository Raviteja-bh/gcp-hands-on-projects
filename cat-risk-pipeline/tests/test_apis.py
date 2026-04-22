"""
Test script to verify API connectivity and data retrieval
"""
from datetime import datetime, timedelta, UTC
import requests


def safe_api_call(url, params=None, timeout=30, retries=3):
    """Make API call with retry logic"""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt + 1}/{retries}")
            if attempt == retries - 1:
                raise
        except requests.exceptions.RequestException as e:
            print(f"Request failed on attempt {attempt + 1}/{retries}: {e}")
            if attempt == retries - 1:
                raise
    return None


def test_usgs_earthquakes(days_back=7):
    """Test USGS earthquake API"""
    print("\n=== USGS Earthquakes ===")
    
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "minmagnitude": 2.5,
        "starttime": (datetime.now(UTC) - timedelta(days=days_back)).strftime("%Y-%m-%d"),
        "endtime": datetime.now(UTC).strftime("%Y-%m-%d"),
        "orderby": "time"
    }
    
    try:
        data = safe_api_call(url, params=params)
        if data and "features" in data:
            count = len(data["features"])
            print(f"USGS: Found {count} earthquakes")
            if count > 0:
                latest = data["features"][0]
                mag = latest["properties"]["mag"]
                place = latest["properties"]["place"]
                print(f"Latest: M {mag} - {place}")
            return True
    except Exception as e:
        print(f"USGS API Error: {e}")
        return False


def test_noaa_alerts(state="FL"):
    """Test NOAA weather alerts API"""
    print("\n=== NOAA Alerts ===")
    
    url = f"https://api.weather.gov/alerts/active"
    params = {"area": state}
    
    try:
        data = safe_api_call(url, params=params)
        if data and "features" in data:
            count = len(data["features"])
            print(f"NOAA: Found {count} active alerts in {state}")
            if count > 0:
                latest = data["features"][0]
                event = latest["properties"]["event"]
                headline = latest["properties"]["headline"]
                print(f"Latest: {headline}")
            return True
    except Exception as e:
        print(f"NOAA API Error: {e}")
        return False


def test_fema_disasters(days_back=30):
    """Test FEMA disaster declarations API"""
    print("\n=== FEMA Disasters ===")
    
    url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
    params = {
        "$filter": f"declarationDate gt '{(datetime.now(UTC) - timedelta(days=days_back)).strftime('%Y-%m-%d')}'"
    }
    
    try:
        data = safe_api_call(url, params=params)
        if data and "DisasterDeclarationsSummaries" in data:
            disasters = data["DisasterDeclarationsSummaries"]
            count = len(disasters)
            print(f"FEMA: Found {count} disaster declarations")
            if count > 0:
                latest = disasters[0]
                title = latest.get("declarationTitle", "N/A")
                state = latest.get("state", "N/A")
                print(f"Latest: {title} - {state}")
            return True
    except Exception as e:
        print(f"FEMA API Error: {e}")
        return False


if __name__ == "__main__":
    print("Testing API Connectivity...")
    print("=" * 50)
    
    usgs_ok = test_usgs_earthquakes(days_back=7)
    noaa_ok = test_noaa_alerts(state="FL")
    fema_ok = test_fema_disasters(days_back=30)
    
    print("\n" + "=" * 50)
    print("Results:")
    print(f"USGS: {'✓' if usgs_ok else '✗'}")
    print(f"NOAA: {'✓' if noaa_ok else '✗'}")
    print(f"FEMA: {'✓' if fema_ok else '✗'}")
    
    if usgs_ok and noaa_ok and fema_ok:
        print("\n✓ All APIs responding correctly!")
    else:
        print("\n✗ Some APIs failed - check errors above")