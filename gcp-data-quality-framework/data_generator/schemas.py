"""
BigQuery schemas for reinsurance data quality framework
"""

# This defines what columns each table has
# Think of it like creating a blueprint for a database table

POLICY_SCHEMA = [
    {"name": "policy_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "policy_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "insured_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "inception_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "expiry_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "premium_amount", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "currency_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "coverage_limit", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "region_code", "type": "STRING", "mode": "NULLABLE"},  # Can be NULL
    {"name": "policy_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

# Valid values for dropdown fields
# These are like "allowed values" - anything else is an error

REGION_CODES = [
    "US-CA", "US-FL", "US-TX", "US-NY", "US-IL",
    "JP-TK", "JP-OS", "EU-UK", "EU-DE", "EU-FR"
]

POLICY_TYPES = [
    "Property", "Casualty", "Marine", "Aviation", "Cyber"
]

CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY"]

PERIL_TYPES = [
    "Hurricane", "Earthquake", "Flood", "Fire", "Windstorm",
    "Hail", "Tornado", "Cyber Attack"
]