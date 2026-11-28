# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed low-volume admission rights that Import Control grants to a source owner."""

ADMISSION_GRANTS = (
    ("provider_directory_api_endpoint", "SELECT(endpoint_id),UPDATE(endpoint_id)"),
    (
        "provider_directory_source",
        "SELECT(source_id,endpoint_id,metadata_json),UPDATE(source_id)",
    ),
    (
        "import_run",
        "SELECT(run_id,retry_of_run_id,engine,node_id,importer,status,finished_at,params),UPDATE(run_id)",
    ),
)
SOURCE_CONTROL_READS = ("provider_directory_source", "provider_directory_api_endpoint")
