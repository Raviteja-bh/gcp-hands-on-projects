import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
import apache_beam.transforms.window as window
from datetime import datetime

# Simulate events with timestamps
events = [
    {"source": "USGS", "location": "California",
     "timestamp": "2026-04-18T10:00:00", "severity": "high"},
    {"source": "USGS", "location": "California",
     "timestamp": "2026-04-18T10:05:00", "severity": "medium"},
    {"source": "NOAA", "location": "Florida",
     "timestamp": "2026-04-18T10:08:00", "severity": "extreme"},
    {"source": "USGS", "location": "Japan",
     "timestamp": "2026-04-18T10:20:00", "severity": "high"},
    # ↑ This one is in the NEXT 15-min window
]

def add_timestamp(event):
    ts = datetime.fromisoformat(event["timestamp"]).timestamp()
    return window.TimestampedValue(event, ts)

def extract_location(event):
    return (event["location"], 1)

with beam.Pipeline() as p:
    (
        p
        | "Create"           >> beam.Create(events)
        | "Add Timestamps"   >> beam.Map(add_timestamp)
        | "Window"           >> beam.WindowInto(FixedWindows(15 * 60))
        | "Extract Location" >> beam.Map(extract_location)
        | "Count"            >> beam.combiners.Count.PerKey()
        | "Print"            >> beam.Map(print)
    )