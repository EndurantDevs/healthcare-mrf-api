# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api import control_workers


def test_provider_directory_worker_targets_its_exact_queued_job():
    spec = control_workers._BY_QUEUE["arq:ProviderDirectoryFHIR"]

    job = control_workers._worker_job_manifest(
        spec,
        {
            "importer": "provider-directory-fhir",
            "run_id": "run_fhir",
            "job_id": "fhir_job_123",
        },
        "ghcr.io/endurantdevs/healthcare-mrf-api:dev",
    )

    container = job["spec"]["template"]["spec"]["containers"][0]
    environment_by_name = {
        item["name"]: item["value"] for item in container["env"]
    }
    assert container["command"][-2:] == [
        "worker-once",
        "process.ProviderDirectoryFHIR",
    ]
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == (
        "fhir_job_123"
    )
