import json
import os
import sys
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import datetime, UTC, timedelta, timezone


# CONFIG
PROJECT_ID = "cat-risk-pipeline-2026"
REGION = "us-central1"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/cat-risk-sub"
BUCKET = f"gs://cat-risk-pipeline-bucket-2026"
BQ_TABLE = f"{PROJECT_ID}:cat_risk.events"

# SCHEMA — must match your schema_events.json exactly
BQ_SCHEMA = {
    "fields": [
        {"name": "source",           "type": "STRING",    "mode": "REQUIRED"},
        {"name": "event_type",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "title",            "type": "STRING",    "mode": "NULLABLE"},
        {"name": "magnitude",        "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "location",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "latitude",         "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "longitude",        "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "severity",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "risk_score",       "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "timestamp",        "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "processed_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "pipeline_version", "type": "STRING",    "mode": "NULLABLE"}
    ]
}



# Transforms (DoFns) for each step of the pipeline
class ParseEventFn(beam.DoFn):
    """
    Step 1 — Parse raw bytes from Pub/Sub into a dict
    """
    def process(self, element, **kwargs):
        try:
            # Pub/Sub sends bytes → decode to string → parse JSON
            event = json.loads(element.decode("utf-8"))
            yield event
        except Exception as e:
            print(f"Failed to parse message: {e}")
            # Don't yield anything → bad message is dropped


class AddTimestampFn(beam.DoFn):
    """
    Step 2 — Tell Beam when this event happened
    so it assigns it to the correct time window
    """
    def process(self, event, **kwargs):
        try:
            ts = datetime.fromisoformat(
                event["timestamp"]
            ).timestamp()
            yield window.TimestampedValue(event, ts)
        except Exception as e:
            print(f"Failed to add timestamp: {e}")


class ValidateEventFn(beam.DoFn):
    """
    Step 3 — Only pass through events with required fields
    """
    REQUIRED_FIELDS = ["source", "event_type", "location", "timestamp"]

    def process(self, event, **kwargs):
        # Check all required fields exist
        missing = [
            f for f in self.REQUIRED_FIELDS
            if not event.get(f)
        ]

        if missing:
            print(f"Dropping invalid event - missing: {missing}")
        else:
            yield event  # ✅ Only yield valid events


class EnrichEventFn(beam.DoFn):
    """
    Step 4 — Add extra fields useful for risk scoring
    """
    # Risk weights by event type
    EVENT_WEIGHTS = {
        "earthquake": 1.5,
        "weather_alert": 1.2,
        "disaster_declaration": 2.0
    }

    def process(self, event, **kwargs):
        event_type = event.get("event_type", "unknown")
        magnitude = event.get("magnitude") or 0
        weight = self.EVENT_WEIGHTS.get(event_type, 1.0)

        # Calculate base risk score (0-100)
        base_score = min(magnitude * weight * 10, 100)

        # Severity multiplier
        severity_multiplier = {
            "extreme": 1.5,
            "high": 1.2,
            "medium": 1.0,
            "low": 0.7,
            "unknown": 1.0
        }.get(event.get("severity", "unknown"), 1.0)

        # Add enriched fields
        event["risk_score"] = round(base_score * severity_multiplier, 2)
        event["processed_at"] = datetime.now(UTC).isoformat()
        event["pipeline_version"] = "1.0"

        yield event


class FormatForBigQueryFn(beam.DoFn):
    """
    NEW — Step 7
    Make sure all fields match BigQuery schema exactly
    """
    def process(self, event, **kwargs):
        yield {
            "source":           event.get("source"),
            "event_type":       event.get("event_type"),
            "title":            event.get("title"),
            "magnitude":        float(event["magnitude"]) if event.get("magnitude") else None,
            "location":         event.get("location"),
            "latitude":         float(event["latitude"]) if event.get("latitude") else None,
            "longitude":        float(event["longitude"]) if event.get("longitude") else None,
            "severity":         event.get("severity"),
            "risk_score":       event.get("risk_score"),
            "timestamp":        event.get("timestamp"),
            "processed_at":     event.get("processed_at"),
            "pipeline_version": event.get("pipeline_version")
        }




# pipeline definition
def run():
    # Pipeline options
    options = PipelineOptions(
        project=PROJECT_ID,
        region=REGION,
        runner='DataflowRunner',
        job_name="cat-risk-streaming-pipeline",
        temp_location=f"{BUCKET}/dataflow-temp",
        staging_location=f"{BUCKET}/dataflow-staging",
        streaming=True,      # ← tells Beam this is a streaming job
        save_main_session=True,
        service_account_email=os.getenv(
                                'DATAFLOW_SERVICE_ACCOUNT', 
                                'pipeline-sa@cat-risk-pipeline-2026.iam.gserviceaccount.com'
                            )
    )

    with beam.Pipeline(options=options) as p:
        events = (
            p
            # Step 1 — Read live messages from Pub/Sub
            | "Read from PubSub"    >> beam.io.ReadFromPubSub(
                                            subscription=SUBSCRIPTION)

            # Step 2 — Parse bytes → dict
            | "Parse Events"        >> beam.ParDo(ParseEventFn())

            # Step 3 — Assign event timestamps
            | "Add Timestamps"      >> beam.ParDo(AddTimestampFn())

            # Step 4 — Apply 15-minute fixed window
            | "Window 15 mins"      >> beam.WindowInto(
                                            FixedWindows(15 * 60),
                                            allowed_lateness=2 * 60)

            # Step 5 — Validate required fields
            | "Validate Events"     >> beam.ParDo(ValidateEventFn())

            # Step 6 — Enrich with risk score
            | "Enrich Events"       >> beam.ParDo(EnrichEventFn())

            # Step 7 — Format for BigQuery
            | "Format for BQ"       >> beam.ParDo(FormatForBigQueryFn())

            # Step 8 — Write to BigQuery ← NEW
            | "Write to BigQuery"   >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    run()