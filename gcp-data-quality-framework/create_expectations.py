import great_expectations as gx
import pandas as pd
import json

# Load GX context
context = gx.get_context(
    mode="file",
    project_root_dir="."
)

# Helper function to load our JSON files
def load_data(filename):
    with open(f"data_generator/{filename}") as f:
        return pd.DataFrame(json.load(f))

print("Loading datasets...")
policies_df = load_data("policies.json")
claims_df = load_data("claims.json")
treaties_df = load_data("treaties.json")
exposures_df = load_data("exposures.json")

print(f"✓ Policies:  {len(policies_df)} records")
print(f"✓ Claims:    {len(claims_df)} records")
print(f"✓ Treaties:  {len(treaties_df)} records")
print(f"✓ Exposures: {len(exposures_df)} records")

# Valid reference data - same as schemas.py
VALID_REGIONS = ["US-CA", "US-FL", "US-TX", "US-NY", "US-IL",
                 "JP-TK", "JP-OS", "EU-UK", "EU-DE", "EU-FR"]
VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
VALID_POLICY_TYPES = ["Property", "Casualty", "Marine", "Aviation", "Cyber"]

# ============================================================
# POLICIES EXPECTATION SUITE
# ============================================================
print("\nCreating Policy Expectation Suite...")

# Step 1: Create empty suite
context.add_or_update_expectation_suite("policies_suite")

# Step 2: Create validator (connects data to suite)
validator = context.get_validator(
    batch_request=gx.core.batch.RuntimeBatchRequest(
        datasource_name="pandas",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="policies",
        runtime_parameters={"batch_data": policies_df},
        batch_identifiers={"default_identifier_name": "policies_batch"},
    ),
    expectation_suite_name="policies_suite"
)

# CHECK 1: Column names exist
# "Make sure all expected columns are present"
validator.expect_table_columns_to_match_ordered_list(
    column_list=[
        "policy_id", "policy_number", "insured_name",
        "inception_date", "expiry_date", "premium_amount",
        "currency_code", "coverage_limit", "region_code",
        "policy_type", "created_at"
    ]
)

# CHECK 2: Primary key must be unique
# "No two policies can have same ID"
validator.expect_column_values_to_be_unique("policy_id")

# CHECK 3: Critical fields cannot be null
# "These fields must always have a value"
validator.expect_column_values_to_not_be_null("policy_id")
validator.expect_column_values_to_not_be_null("premium_amount")

# CHECK 4: Premium must be positive
# "You can't charge negative premium!"
validator.expect_column_values_to_be_between(
    "premium_amount",
    min_value=0,      # Minimum allowed value
    max_value=None    # No maximum limit
)

# CHECK 5: Currency must be valid
# "Only these 4 currencies allowed"
validator.expect_column_values_to_be_in_set(
    "currency_code",
    VALID_CURRENCIES  # ["USD", "EUR", "GBP", "JPY"]
)

# CHECK 6: Region must be valid (95% threshold)
# "Allow 5% missing - data arrives before region assigned"
validator.expect_column_values_to_be_in_set(
    "region_code",
    VALID_REGIONS,
    mostly=0.95       # 95% must pass, 5% can fail
)

# CHECK 7: Coverage must be greater than premium
# "Business rule: coverage limit > premium always"
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="coverage_limit",
    column_B="premium_amount"
)

# Save the suite
validator.save_expectation_suite(discard_failed_expectations=False)
print("✓ Policies suite created!")

# ============================================================
# CLAIMS EXPECTATION SUITE
# ============================================================
print("\nCreating Claims Expectation Suite...")

context.add_or_update_expectation_suite("claims_suite")

validator = context.get_validator(
    batch_request=gx.core.batch.RuntimeBatchRequest(
        datasource_name="pandas",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="claims",
        runtime_parameters={"batch_data": claims_df},
        batch_identifiers={"default_identifier_name": "claims_batch"},
    ),
    expectation_suite_name="claims_suite"
)

# Uniqueness
validator.expect_column_values_to_be_unique("claim_id")

# Nulls
validator.expect_column_values_to_not_be_null("claim_id")
validator.expect_column_values_to_not_be_null("policy_id")
validator.expect_column_values_to_not_be_null("loss_amount")

# Loss amount must be positive
validator.expect_column_values_to_be_between(
    "loss_amount",
    min_value=0,
    max_value=None
)

# Paid amount must be positive
validator.expect_column_values_to_be_between(
    "paid_amount",
    min_value=0,
    max_value=None
)

# Business rule: paid cannot exceed loss
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="loss_amount",
    column_B="paid_amount",
    or_equal=True  # paid CAN equal loss (fully paid claim)
)

# Referential integrity: policy_id must exist in policies
valid_policy_ids = set(policies_df['policy_id'])
validator.expect_column_values_to_be_in_set(
    "policy_id",
    valid_policy_ids,
    mostly=0.95  # Allow 5% orphaned claims
)

# Claim status must be valid
validator.expect_column_values_to_be_in_set(
    "claim_status",
    ["Open", "Closed", "Pending", "Settled"]
)

validator.save_expectation_suite(discard_failed_expectations=False)
print("✓ Claims suite created!")

# ============================================================
# TREATIES EXPECTATION SUITE
# ============================================================
print("\nCreating Treaties Expectation Suite...")

context.add_or_update_expectation_suite("treaties_suite")

validator = context.get_validator(
    batch_request=gx.core.batch.RuntimeBatchRequest(
        datasource_name="pandas",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="treaties",
        runtime_parameters={"batch_data": treaties_df},
        batch_identifiers={"default_identifier_name": "treaties_batch"},
    ),
    expectation_suite_name="treaties_suite"
)

# Core checks
validator.expect_column_values_to_be_unique("treaty_id")
validator.expect_column_values_to_not_be_null("treaty_id")

# Business Rule: Retention amount must be less than or equal to the limit amount
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="limit_amount",
    column_B="retention_amount",
    or_equal=True
)

# Business Rule: Ceding commission must be a realistic percentage (0% to 100%)
validator.expect_column_values_to_be_between(
    "ceding_commission",
    min_value=0.0,
    max_value=1.0
)

validator.save_expectation_suite(discard_failed_expectations=False)
print("✓ Treaties suite created!")

# ============================================================
# EXPOSURES EXPECTATION SUITE
# ============================================================
print("\nCreating Exposures Expectation Suite...")

context.add_or_update_expectation_suite("exposures_suite")

validator = context.get_validator(
    batch_request=gx.core.batch.RuntimeBatchRequest(
        datasource_name="pandas",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="exposures",
        runtime_parameters={"batch_data": exposures_df},
        batch_identifiers={"default_identifier_name": "exposures_batch"},
    ),
    expectation_suite_name="exposures_suite"
)

# Core checks
validator.expect_column_values_to_be_unique("exposure_id")
validator.expect_column_values_to_not_be_null("exposure_id")

# Business Rule: Total Insured Value (TIV) must be positive
validator.expect_column_values_to_be_between(
    "total_insured_value",
    min_value=0,
    max_value=None
)

validator.save_expectation_suite(discard_failed_expectations=False)
print("✓ Exposures suite created!")