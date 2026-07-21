# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api import control_imports
from process import provider_directory_profile_selection as selection


def _profile_params_map() -> dict:
    identity_map = {
        "contract_id": selection.PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
        "node_id": "dev-node",
        "catalog_digest": "a" * 64,
        "selection_fingerprint": "b" * 64,
        "authority_revision": 7,
        "profile_schema_version": 1,
        "profile_strategy_version": "strategy-v1",
        "source_context_digest": "c" * 64,
        "profile_input_digest": "d" * 64,
        "operation": "publish",
        "pairs": [
            {
                "source_id": "pdfhir_payer",
                "endpoint_id": "endpoint-1",
                "dataset_id": "dataset-1",
                "dataset_hash": "e" * 64,
                "acquisition_root_run_id": "run-root-1",
                "publication_status": "published",
                "is_current": True,
                "lineage_authority": selection.PROFILE_SELECTION_LINEAGE_AUTHORITY,
            }
        ],
    }
    attestation_map = {
        **identity_map,
        "proof_id": selection._proof_id(identity_map),
    }
    return {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": 11,
        "provider_directory_profile_selection_attestation": attestation_map,
    }


def test_global_profile_serializes_with_source_context_seed_changes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    profile_params_map = _profile_params_map()
    seed_params_map = {
        "seed_only": True,
        "source_ids": ["pdfhir_payer"],
    }
    active_seed_map = {
        "run_id": "run-seed",
        "params": seed_params_map,
        "metrics": {},
    }
    active_profile_map = {
        "run_id": "run-profile",
        "params": profile_params_map,
        "metrics": {},
    }

    assert control_imports._provider_directory_blocking_run(
        profile_params_map,
        [active_seed_map],
    ) == active_seed_map
    assert control_imports._provider_directory_blocking_run(
        seed_params_map,
        [active_profile_map],
    ) == active_profile_map
