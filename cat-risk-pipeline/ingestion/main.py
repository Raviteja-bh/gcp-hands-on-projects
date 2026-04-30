import json
import time
import requests
from datetime import datetime, timedelta, timezone
from google.cloud import pubsub_v1


#config
PROJECT_ID = "cat-risk-pipeline-2026"
TOPIC_ID = "cat-risk-raw-events"


#PUB/SUB PUBLISHER FUNCTION

def publish_event(publisher, topic_path, event):
    """
    Publish a single event to Pub/Sub topic
    """
    # Convert dict to JSON string → encode to bytes
    message_bytes = json.dumps(event).encode("utf-8")

    future = publisher.publish(topic_path, message_bytes)
    return future.result()  # Wait for publish to complete


# API Wrapper with retry logic
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
                time.sleep(2 ** attempt)
            else:
                print(f"Error {response.status_code} from {url}")
                return None
        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError:
            print(f"Connection error on attempt {attempt + 1}")
    return None


# Fetch functions for each data source, normalizing to a common event format

def fetch_earthquakes():
    data = safe_api_call(
        url="https://earthquake.usgs.gov/fdsnws/event/1/query",
        params={
            "format": "geojson",
            "starttime": (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": 2.5,
            "orderby": "time"
        }
    )
    if data is None:
        return []

    # Normalize to our standard event format
    events = []
    for feature in data["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        events.append({
            "source": "USGS",
            "event_type": "earthquake",
            "title": props.get("title"),
            "magnitude": props.get("mag"),
            "location": props.get("place"),
            "latitude": coords[1],
            "longitude": coords[0],
            "severity": "high" if props.get("mag", 0) >= 5.0 else "medium",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    return events


def fetch_noaa_alerts():
    data = safe_api_call(
        url="https://api.weather.gov/alerts/active",
        params={"status": "actual"},
        headers={
            "User-Agent": "cat-risk-pipeline (test@email.com)",
            "Accept": "application/json"
        }
    )
    if data is None:
        return []

    events = []
    for feature in data["features"]:
        props = feature["properties"]
        events.append({
            "source": "NOAA",
            "event_type": "weather_alert",
            "title": props.get("headline"),
            "magnitude": None,
            "location": props.get("areaDesc"),
            "latitude": None,
            "longitude": None,
            "severity": props.get("severity", "unknown").lower(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    return events


def fetch_fema_disasters():
    data = safe_api_call(
        url="https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
        params={
            "$top": 10,
            "$orderby": "declarationDate desc",
            "$filter": f"declarationDate gt '{(datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')}'"
        },
        headers={
            "Accept": "application/json",
            "User-Agent": "cat-risk-pipeline (test@email.com)"
        }
    )
    if data is None:
        return []

    events = []
    for disaster in data["DisasterDeclarationsSummaries"]:
        events.append({
            "source": "FEMA",
            "event_type": "disaster_declaration",
            "title": disaster.get("declarationTitle"),
            "magnitude": None,
            "location": disaster.get("state"),
            "latitude": None,
            "longitude": None,
            "severity": "high",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    return events


# Entry point for GCP Cloud Function

def main(request):
    """
    Main entry point for Cloud Function.
    Fetches all catastrophe events and publishes to Pub/Sub.
    """
    print(f"Starting ingestion at {datetime.now(timezone.utc).isoformat()}")

    # Initialize Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # Fetch all events
    all_events = []
    all_events.extend(fetch_earthquakes())
    all_events.extend(fetch_noaa_alerts())
    all_events.extend(fetch_fema_disasters())

    print(f"Total events fetched: {len(all_events)}")

    # Publish each event to Pub/Sub
    published_count = 0
    for event in all_events:
        try:
            publish_event(publisher, topic_path, event)
            published_count += 1
        except Exception as e:
            print(f"Failed to publish event: {e}")

    result = f"Published {published_count}/{len(all_events)} events to Pub/Sub"
    print(result)
    return result