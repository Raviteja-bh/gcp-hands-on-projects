import great_expectations as gx
import pandas as pd
import json
from great_expectations.checkpoint import Checkpoint

# 1. Load GX context
context = gx.get_context(mode="file", project_root_dir=".")

# 2. Helper to load data
def load_data(filename):
    with open(f"data_generator/{filename}") as f:
        return pd.DataFrame(json.load(f))

# 3. Load active data
policies_df = load_data("policies.json")
claims_df = load_data("claims.json")
treaties_df = load_data("treaties.json")
exposures_df = load_data("exposures.json")

# 4. Define the runtime batches
policies_batch = gx.core.batch.RuntimeBatchRequest(
    datasource_name="pandas",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="policies",
    runtime_parameters={"batch_data": policies_df},
    batch_identifiers={"default_identifier_name": "policies_batch"},
)

claims_batch = gx.core.batch.RuntimeBatchRequest(
    datasource_name="pandas",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="claims",
    runtime_parameters={"batch_data": claims_df},
    batch_identifiers={"default_identifier_name": "claims_batch"},
)

treaties_batch = gx.core.batch.RuntimeBatchRequest(
    datasource_name="pandas",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="treaties",
    runtime_parameters={"batch_data": treaties_df},
    batch_identifiers={"default_identifier_name": "treaties_batch"},
)

exposures_batch = gx.core.batch.RuntimeBatchRequest(
    datasource_name="pandas",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="exposures",
    runtime_parameters={"batch_data": exposures_df},
    batch_identifiers={"default_identifier_name": "exposures_batch"},
)

# 🏁 5. Define a clean Checkpoint skeleton (without live data)
checkpoint = context.add_or_update_checkpoint(
    name="insurance_checkpoint"
)

# 🚀 6. Run validation by passing the active validations at runtime
print("\nRunning data quality validation checkpoint...")
checkpoint_result = checkpoint.run(
    validations=[
        {"batch_request": policies_batch, "expectation_suite_name": "policies_suite"},
        {"batch_request": claims_batch, "expectation_suite_name": "claims_suite"},
        {"batch_request": treaties_batch, "expectation_suite_name": "treaties_suite"},
        {"batch_request": exposures_batch, "expectation_suite_name": "exposures_suite"},
    ]
)

print("Building Data Docs...")
context.build_data_docs()
print("✓ Validation complete! Data Docs updated.")