import great_expectations as gx

# Connect to our GX context
context = gx.get_context(
    mode="file",
    project_root_dir="."  # Current directory
)

# Add pandas datasource
# Pandas = reads data from files (JSON, CSV etc.)
# Later we'll add BigQuery datasource for GCP
datasource_config = {
    "name": "pandas",                    # Name we'll use to reference it
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine"  # Use pandas to read data
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        }
    }
}

context.add_datasource(**datasource_config)
print("✓ Pandas datasource added successfully")
print(f"Available datasources: {list(context.datasources.keys())}")