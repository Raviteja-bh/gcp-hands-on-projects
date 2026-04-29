import json
import time
from google.cloud import pubsub_v1

PROJECT_ID = "cat-risk-pipeline-2026"
TOPIC_ID = "cat-risk-raw-events"
SUBSCRIPTION_ID = "cat-risk-sub"

# ── PUBLISH ──────────────────────────────────────────
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

test_event = {
    "source": "USGS",
    "event_type": "earthquake",
    "magnitude": 6.1,
    "location": "Tokyo, Japan",
    "severity": "high",
    "timestamp": "2026-04-18T10:00:00"
}

print("=== PUBLISHING ===")
future = publisher.publish(
    topic_path,
    json.dumps(test_event).encode("utf-8")
)
print(f"Published! Message ID: {future.result()}")

# Small wait for message to arrive
time.sleep(2)

# ── PULL ─────────────────────────────────────────────
print("\n=== PULLING ===")
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)

response = subscriber.pull(
    request={
        "subscription": subscription_path,
        "max_messages": 5
    }
)

for msg in response.received_messages:
    data = json.loads(msg.message.data.decode("utf-8"))
    print(f"Received event from: {data['source']}")
    print(f"Location: {data['location']}")
    print(f"Magnitude: {data['magnitude']}")
    print(f"Severity: {data['severity']}")

# ACK messages
ack_ids = [m.ack_id for m in response.received_messages]
if ack_ids:
    subscriber.acknowledge(
        request={
            "subscription": subscription_path,
            "ack_ids": ack_ids
        }
    )
    print(f"\nACKed {len(ack_ids)} messages ✅")