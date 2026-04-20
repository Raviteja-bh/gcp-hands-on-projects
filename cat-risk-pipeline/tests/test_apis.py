import requests
from datetime import datetime, timedelta

# Test 1 — USGS
print("=== USGS Earthquakes ===")
response = requests.get(
    "https://earthquake.usgs.gov/fdsnws/event/1/query",
    params={"format": "geojson", "minmagnitude": 5.0,
            "starttime": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")}
)
data = response.json()
print(f"Earthquakes found: {len(data['features'])}")
print(f"Latest: {data['features'][0]['properties']['title']}")

# Test 2 — NOAA
print("\n=== NOAA Alerts ===")
response = requests.get(
    "https://api.weather.gov/alerts/active",
    params={"status": "actual"},
    headers={"User-Agent": "cat-risk-pipeline (test@email.com)"}
)
data = response.json()
print(f"Active alerts found: {len(data['features'])}")

# Test 3 — FEMA (corrected endpoint)
print("\n=== FEMA Disasters ===")
response = requests.get(
    "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
    # ↑ Capital D, Capital S — this was the bug
    params={
        "$top": 5,
        "$orderby": "declarationDate desc",
        "$filter": "declarationDate gt '2026-01-01'"
    },
    headers={
        "Accept": "application/json",
        "User-Agent": "cat-risk-pipeline (test@email.com)"
    }
)

print(f"Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    disasters = data["DisasterDeclarationsSummaries"]
    print(f"Recent disasters found: {len(disasters)}")
    if disasters:
        first = disasters[0]
        print(f"Latest: {first['declarationTitle']} - {first['state']}")
else:
    print(f"Error: {response.text[:200]}")