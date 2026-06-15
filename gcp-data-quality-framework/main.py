import os
import io
import logging
import functions_framework
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMAS = {
    "policies": [
        bigquery.SchemaField("policy_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("policy_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("insured_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("inception_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("expiry_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("premium_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("coverage_limit", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("region_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("policy_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "claims": [
        bigquery.SchemaField("claim_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("policy_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("claim_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loss_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("reported_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("loss_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("paid_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("reserve_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("claim_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("peril_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "treaties": [
        bigquery.SchemaField("treaty_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("treaty_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("treaty_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("effective_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("expiration_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("retention_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("limit_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ceding_commission", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("territory", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "exposures": [
        bigquery.SchemaField("exposure_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("region_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("peril_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("total_insured_value", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("number_of_policies", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("average_premium", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("as_of_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]
}

@functions_framework.cloud_event
def validate_insurance_data(cloudevent):
    import pandas as pd
    import great_expectations as gx
    import great_expectations.expectations as gxe

    storage_client = storage.Client()
    bq_client = bigquery.Client()
    
    gcp_project_id = bq_client.project
    archive_bucket = f"{gcp_project_id}-archive-zone"
    quarantine_bucket = f"{gcp_project_id}-quarantine-zone"
    docs_bucket_name = f"{gcp_project_id}-gx-docs"
    bq_dataset = "insurance_analytics"

    event_data = cloudevent.data
    bucket_name = event_data['bucket']
    file_name = event_data['name']
    
    logger.info(f"⚡ Processing file delivery event: gs://{bucket_name}/{file_name}")
    
    asset_type = None
    for key in SCHEMAS.keys():
        if key in file_name.lower():
            asset_type = key
            break

    if not asset_type:
        logger.error(f"❌ Aborting. Cannot map filename '{file_name}' to known schemas.")
        return

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(file_name)

    try:
        raw_text_data = source_blob.download_as_text()
        try:
            df = pd.read_json(io.StringIO(raw_text_data))
        except Exception:
            df = pd.read_json(io.StringIO(raw_text_data), lines=True)
    except Exception as e:
        logger.error(f"❌ Completely malformed JSON structure. Quarantining: {str(e)}")
        target_quarantine_bucket = storage_client.bucket(quarantine_bucket)
        source_bucket.copy_blob(source_blob, target_quarantine_bucket, file_name)
        source_blob.delete()
        return

    context = gx.get_context(mode="ephemeral")
    suite = gx.ExpectationSuite(name=f"{asset_type}_suite")
    
    id_column = "exposure_id" if asset_type == "exposures" else f"{asset_type[:-1]}_id" 
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=id_column))
    
    if asset_type == "policies":
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="premium_amount", min_value=0))
    elif asset_type == "claims":
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="loss_amount", min_value=0))
        
    suite = context.suites.add(suite)

    data_source = context.data_sources.add_pandas(name="gcs_data_source")
    data_asset = data_source.add_dataframe_asset(name=f"{asset_type}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def")
    
    validation_definition = gx.ValidationDefinition(
        name=f"validate_{asset_type}",
        data=batch_definition,
        suite=suite
    )
    validation_definition = context.validation_definitions.add(validation_definition)
    
    # --- CRITICAL OPTIMIZATION: Pass COMPLETE format to get row indices ---
    validation_result = validation_definition.run(
        batch_parameters={"dataframe": df},
        result_format={"result_format": "COMPLETE"}
    )
    
    bad_indices = set()
    for res in validation_result.results:
        if not res.success:
            unexpected_indices = res.result.get("unexpected_index_list", []) or []
            bad_indices.update(unexpected_indices)

    if bad_indices:
        logger.warning(f"⚠️ Found {len(bad_indices)} quality validation errors.")
        df_bad = df.loc[list(bad_indices)].copy()
        df_clean = df.drop(index=list(bad_indices)).copy()
    else:
        logger.info("✨ Clean sheet! 0 quality violations detected.")
        df_bad = pd.DataFrame(columns=df.columns)
        df_clean = df.copy()

    # Ingest clean rows to BigQuery
    if not df_clean.empty:
        table_id = f"{gcp_project_id}.{bq_dataset}.{asset_type}"
        
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMAS[asset_type], 
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )
        
        for field in SCHEMAS[asset_type]:
            if field.name not in df_clean.columns:
                df_clean[field.name] = None
            elif field.field_type == "TIMESTAMP" and df_clean[field.name].notna().any():
                converted_dt = pd.to_datetime(df_clean[field.name], errors='coerce')
                if converted_dt.dt.tz is None:
                    df_clean[field.name] = converted_dt.dt.tz_localize('UTC')
                else:
                    df_clean[field.name] = converted_dt.dt.tz_convert('UTC')
        
        schema_columns = [field.name for field in SCHEMAS[asset_type]]
        df_clean = df_clean[schema_columns]
        
        try:
            job = bq_client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
            job.result()
            logger.info(f"🏛️ Successfully appended {len(df_clean)} clean rows to BigQuery.")
        except Exception as bq_err:
            logger.error(f"❌ BigQuery Database structural write rejection: {str(bq_err)}")
            df_bad = pd.concat([df_bad, df_clean]).drop_duplicates()

    if not df_bad.empty:
        quarantine_file_name = f"isolated_corrupt_rows_{file_name}"
        quarantine_bucket_obj = storage_client.bucket(quarantine_bucket)
        bad_blob = quarantine_bucket_obj.blob(quarantine_file_name)
        bad_blob.upload_from_string(df_bad.to_json(orient='records', lines=True), content_type='application/json')
        logger.info(f"☣️ Isolated corrupt rows to quarantine.")

    try:
        docs_results = context.build_data_docs()
        local_site_url = docs_results.get("local_site")
        if local_site_url and local_site_url.startswith("file://"):
            local_index_path = local_site_url.replace("file://", "")
            local_docs_root = os.path.dirname(local_index_path)
            
            docs_bucket = storage_client.bucket(docs_bucket_name)
            
            for root, _, files in os.walk(local_docs_root):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, local_docs_root)
                    
                    content_type = "text/html"
                    if file.endswith(".css"):
                        content_type = "text/css"
                    elif file.endswith(".js"):
                        content_type = "application/javascript"
                    elif file.endswith(".json"):
                        content_type = "application/json"
                        
                    blob = docs_bucket.blob(relative_path)
                    blob.upload_from_filename(local_file_path, content_type=content_type)
            logger.info(f"📊 Data Docs site pushed to cloud storage bucket: gs://{docs_bucket_name}")
    except Exception as docs_err:
        logger.error(f"⚠️ Failed to compile or move Data Docs: {str(docs_err)}")

    archive_bucket_obj = storage_client.bucket(archive_bucket)
    source_bucket.copy_blob(source_blob, archive_bucket_obj, file_name)
    source_blob.delete()
    logger.info(f"🗄️ Moved original intake file to archive history.")