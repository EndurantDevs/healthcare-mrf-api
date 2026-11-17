# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for retained CMS guards and the partitioned source layout."""

from __future__ import annotations

import hashlib

from process.network_address_projection import _identifier
from process.provider_directory_cms_receipt_guard import _normalized
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings

_TABLES = (
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_npd_relationship",
    "provider_directory_cms_npd_relationship_receipt",
    "provider_directory_cms_candidate_coverage",
    "provider_directory_cms_serving_coverage",
)
# Exact existing migration bodies, normalized only for schema and whitespace.
_FUNCTION_HASHES = {
    "guard_cms_npd_candidate_coverage_insert": "bad370fc74c2c776fc8cb71aacb8382cc7f70620b9b0ff30f14de577c4f737f0",
    "guard_cms_npd_covered_network_witness": "62c6954d5d738ecc6439ca91be307ee6b3d072eae06ca1182dcab6860cbafb8c",
    "guard_cms_npd_covered_truncate": "9227b9ac1bdb9983a140b0b1d7399217cbdd539b20820a640bd533818203d1bf",
    "guard_cms_npd_published_relationship": "a4f96ca6c5ab1088e7d213af6134f25262225244f998bc0fe502e79dc686d2ca",
    "guard_cms_npd_published_relationship_insert": "4b72e095c2e4dbcf724256cc6f2a5ac2bfad20ea4b26c9175b45fd084e124fdb",
    "guard_cms_npd_published_resource": "dc433262879f5c27a8caea8b1931c65e88de2a04988c6f8d91c6719fadc00d01",
    "guard_cms_npd_relationship_receipt_insert": "f6cd41686b208976aac3170db032262084c2ab8ab602cc29e6539c2566a73773",
    "guard_cms_npd_relationship_receipt_mutation": "8fb51e8daeb9cedf8f4b913801995269039afc6c7167b44d789ce907c151b492",
    "guard_cms_npd_relationship_truncate": "5eff6deb2101ec6d3c051120fcc15dabeac32cb98840b6ec3c5c2d733553bcd5",
    "guard_cms_npd_resource_witness": "cd20e101c89562cd7c66b4e8e3675c57b2c3b6dee9d06a6b19ee654545bd890f",
    "guard_cms_npd_resource_witness_insert": "20ea7e3970f2ce94582ecdf307b56e77570e4f8254c2c3fb00c6f522e11f8e95",
    "guard_cms_npd_serving_coverage": "39e3d09c22c771e91505306456bfbc34ccd84f78464185c1418eed8f7ae4e9e3",
    "guard_cms_npd_serving_evidence": "20cd7e1a76eedb4442514347100b634cba0e91bc0cf3b488b26077c6cfb84689",
    "guard_provider_directory_endpoint_dataset_admission_seal": "fb032fcececbe933ebff7090f69a7d62a9b0387cc94c34a285ef0e84189989ef",
    "guard_tin_npi_connector_endpoint_dataset": "3de37e04d00a51a5ac205a6780968e535f5ebe804c9d7d37ac46ee69e15e8c8c",
}
_GUARDS = {
    "provider_directory_endpoint_dataset": {
        "tin_npi_connector_endpoint_dataset_guard": 31,
        "provider_directory_endpoint_dataset_admission_seal_guard": 23,
        "provider_directory_endpoint_dataset_admission_raw_guard": 19,
        "cms_npd_covered_truncate_forbidden": 34,
    },
    "provider_directory_dataset_resource": {
        "cms_npd_published_resource_immutable": 4,
        "cms_npd_published_resource_update_immutable": 16,
        "cms_npd_published_resource_delete_immutable": 8,
        "cms_npd_covered_truncate_forbidden": 34,
    },
    "provider_directory_cms_npd_resource_witness": {
        "cms_npd_resource_witness_insert_guard": 4,
        "cms_npd_resource_witness_guard": 27,
        "cms_npd_resource_witness_no_truncate": 34,
    },
    "provider_directory_cms_npd_relationship": {
        "cms_npd_published_relationship_insert_immutable": 4,
        "cms_npd_published_relationship_immutable": 27,
        "cms_npd_relationship_truncate_forbidden": 34,
    },
    "provider_directory_cms_npd_relationship_receipt": {
        "cms_npd_relationship_receipt_insert_guard": 4,
        "cms_npd_relationship_receipt_mutation_guard": 27,
        "cms_npd_relationship_receipt_no_truncate": 34,
    },
    "provider_directory_cms_candidate_coverage": {
        "cms_npd_candidate_coverage_insert": 7,
        "cms_npd_candidate_coverage_immutable": 58,
    },
    "provider_directory_cms_serving_coverage": {
        "cms_npd_serving_coverage_immutable": 27,
        "cms_npd_covered_truncate_forbidden": 34,
    },
}
for _table in _TABLES[2:6]:
    _GUARDS[_table] = {"cms_npd_serving_evidence_immutable": 27, "cms_npd_covered_truncate_forbidden": 34}
_GUARDS["provider_directory_insurance_network_plan_evidence"]["cms_npd_covered_network_witness_immutable"] = 4
_GUARD_FUNCTIONS = {
    "tin_npi_connector_endpoint_dataset_guard": "guard_tin_npi_connector_endpoint_dataset",
    "provider_directory_endpoint_dataset_admission_seal_guard": "guard_provider_directory_endpoint_dataset_admission_seal",
    "provider_directory_endpoint_dataset_admission_raw_guard": "guard_provider_directory_endpoint_dataset_admission_seal",
    "cms_npd_covered_truncate_forbidden": "guard_cms_npd_covered_truncate",
    "cms_npd_published_resource_immutable": "guard_cms_npd_published_resource",
    "cms_npd_published_resource_update_immutable": "guard_cms_npd_published_resource",
    "cms_npd_published_resource_delete_immutable": "guard_cms_npd_published_resource",
    "cms_npd_resource_witness_insert_guard": "guard_cms_npd_resource_witness_insert",
    "cms_npd_resource_witness_guard": "guard_cms_npd_resource_witness",
    "cms_npd_resource_witness_no_truncate": "guard_cms_npd_resource_witness",
    "cms_npd_published_relationship_insert_immutable": "guard_cms_npd_published_relationship_insert",
    "cms_npd_published_relationship_immutable": "guard_cms_npd_published_relationship",
    "cms_npd_relationship_truncate_forbidden": "guard_cms_npd_relationship_truncate",
    "cms_npd_relationship_receipt_insert_guard": "guard_cms_npd_relationship_receipt_insert",
    "cms_npd_relationship_receipt_mutation_guard": "guard_cms_npd_relationship_receipt_mutation",
    "cms_npd_relationship_receipt_no_truncate": "guard_cms_npd_relationship_truncate",
    "cms_npd_candidate_coverage_insert": "guard_cms_npd_candidate_coverage_insert",
    "cms_npd_candidate_coverage_immutable": "guard_cms_npd_serving_coverage",
    "cms_npd_serving_coverage_immutable": "guard_cms_npd_serving_coverage",
    "cms_npd_serving_evidence_immutable": "guard_cms_npd_serving_evidence",
    "cms_npd_covered_network_witness_immutable": "guard_cms_npd_covered_network_witness",
}
_UPDATE_COLUMNS = {
    "tin_npi_connector_endpoint_dataset_guard": (
        "dataset_id",
        "endpoint_id",
        "import_run_id",
        "acquisition_root_run_id",
        "previous_dataset_id",
        "dataset_hash",
        "status",
        "is_current",
        "resource_count",
        "created_at",
        "validated_at",
        "published_at",
        "superseded_at",
        "publication_metadata_json",
        "completion_proof_required_version",
        "completion_proof_json",
        "completion_proof_sha256",
    ),
    "provider_directory_endpoint_dataset_admission_seal_guard": (
        "publication_metadata_summary_json",
        "publication_metadata_sha256",
        "content_proof_admission_version",
        "content_proof_admission_kind",
        "content_proof_admission_sha256",
        "content_proof_resource_types",
    ),
    "provider_directory_endpoint_dataset_admission_raw_guard": ("publication_metadata_json",),
}
_TRANSITION_TABLES = {
    "cms_npd_published_resource_immutable": (None, "changed_new"),
    "cms_npd_published_resource_update_immutable": ("changed_old", "changed_new"),
    "cms_npd_published_resource_delete_immutable": ("changed_old", None),
    "cms_npd_resource_witness_insert_guard": (None, "inserted_witness_rows"),
    "cms_npd_published_relationship_insert_immutable": (None, "inserted"),
    "cms_npd_relationship_receipt_insert_guard": (None, "inserted"),
    "cms_npd_covered_network_witness_immutable": (None, "inserted_witnesses"),
}


# Source-bound bodies reachable from the retained input guards and two read-only validators.
_REACHABLE_FUNCTIONS = {
    "guard_cms_npd_candidate_coverage_insert": (
        "bad370fc74c2c776fc8cb71aacb8382cc7f70620b9b0ff30f14de577c4f737f0",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_covered_network_witness": (
        "62c6954d5d738ecc6439ca91be307ee6b3d072eae06ca1182dcab6860cbafb8c",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_covered_truncate": (
        "9227b9ac1bdb9983a140b0b1d7399217cbdd539b20820a640bd533818203d1bf",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_published_relationship": (
        "a4f96ca6c5ab1088e7d213af6134f25262225244f998bc0fe502e79dc686d2ca",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_published_relationship_insert": (
        "4b72e095c2e4dbcf724256cc6f2a5ac2bfad20ea4b26c9175b45fd084e124fdb",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_published_resource": (
        "dc433262879f5c27a8caea8b1931c65e88de2a04988c6f8d91c6719fadc00d01",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_relationship_receipt_insert": (
        "f6cd41686b208976aac3170db032262084c2ab8ab602cc29e6539c2566a73773",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_relationship_receipt_mutation": (
        "8fb51e8daeb9cedf8f4b913801995269039afc6c7167b44d789ce907c151b492",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_relationship_truncate": (
        "5eff6deb2101ec6d3c051120fcc15dabeac32cb98840b6ec3c5c2d733553bcd5",
        "plpgsql",
        "v",
        False,
        None,
    ),
    "guard_cms_npd_resource_witness": (
        "cd20e101c89562cd7c66b4e8e3675c57b2c3b6dee9d06a6b19ee654545bd890f",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_resource_witness_insert": (
        "20ea7e3970f2ce94582ecdf307b56e77570e4f8254c2c3fb00c6f522e11f8e95",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_serving_coverage": (
        "39e3d09c22c771e91505306456bfbc34ccd84f78464185c1418eed8f7ae4e9e3",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_serving_evidence": (
        "20cd7e1a76eedb4442514347100b634cba0e91bc0cf3b488b26077c6cfb84689",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_pd_endpoint_dataset_subset_replay_evidence": (
        "9b47ae0624b49364abc40fed3351a76449bfb9cb768e65de4ade8a27745e2250",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_pd_uhc_flex_practitioner_dataset_parent": (
        "99f559694a2d9a3d0c80c612c329256954b6f61a04f4e2aa47714206ba9b124d",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_endpoint_dataset_admission_seal": (
        "fb032fcececbe933ebff7090f69a7d62a9b0387cc94c34a285ef0e84189989ef",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_endpoint_dataset_admission_truncate": (
        "67f7ec10073087b166a27c901630edbbce23f3e19ab86194a68958e7b7c2cc65",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_exact_logical_current": (
        "2c0604aad0855b862aa4e79ff1b60454f77670fbd4643f098c85348cf00ebe48",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_reviewed_subset_activation_dataset": (
        "2817f524c8949aaed6542a106e9531d08d38a8b59de0ae75e4fce45ad7a3747d",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_rooted_graph_dependency": (
        "05d6db81871ba73f3dcf2e52a07626ca5b1f12232bd1a7357e7fe1bd9d9c1999",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_subset_abandonment_child": (
        "a23ecc5a9a36d864f60626f93d5373ebc71edebb41c27ce1236ff1201f0eb21d",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_subset_abandonment_dataset": (
        "57d04276ddfeccb4e18803b011820be53afab001ba91f87c1a5e5c831e796205",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_terminal_root_retirement_child": (
        "fa606094c8ba8fb2b38d44fed8fc7ab7e291a593140603684fbe08f4a652533a",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_terminal_root_retirement_parent": (
        "b21b66d6c03896ff7277e164a9acb8e173ad90566053958db0ba055ecf2bef56",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_tin_npi_connector_dataset_resource": (
        "1df3ebb4c1a3396d5718cae4b498e1df4fce7240d7de5d9018f18e32d6d83ccb",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_tin_npi_connector_endpoint_dataset": (
        "3de37e04d00a51a5ac205a6780968e535f5ebe804c9d7d37ac46ee69e15e8c8c",
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "pd_entity_redirect_parent_guard": (
        "255490a40d94e95afa9dfadd1cd4f3617f59784c2211949062fe8f087adfc5aa",
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_endpoint_dataset_admission_metadata_sha256": (
        "04f991988414f3923bf0a17ce6aaa9405333ae7617df79a3418b1d9fb1edc94c",
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_rooted_graph_dataset_intrinsic_valid": (
        "a9367bb3a6cb22418d405de2a6421cd634812680d5e3e558785355a7c8674eb2",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_abandonment_valid": (
        "ea2848c774ba934fa2b77bf1b497c11aba98d73123c959b2685440ab89eccbea",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_canonical_sha256": (
        "1354b398c55e84120a6b36a5fcc0a3fcadc060968b8cd989a9763cb44675e3f7",
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_completion_canonical_json": (
        "32ea8489eb3b124c3c2ec30223bf98f2ef708d25b59436da192dbdbde1290aef",
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_completion_proof_pair_valid": (
        "696568744be7aed8c78d584f5e2876777d8d306978a2833e4208092e551dc429",
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_completion_proof_shape_valid": (
        "6c4bbb194d1c7d5b64d8e6e4e2cafb839db5603eba0c4a9c0d8afd3334f4d7b4",
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_content_proof_valid": (
        "6828149dbb0cf7e8e10f7a17282cde8570ee2193f34616400a36540eb380191c",
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_coverage_shape_valid": (
        "fdea7f15087b082e91e5e7cad9e3768eb93a423e80ee6389d3ef76627e8cd678",
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_payload_canonical_json": (
        "712e00577124db45bf1bf4c95c69374c328fa5a417d70bd2306c304658871ae8",
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_payload_sha256": (
        "3b0b71c683fc342e223c9d1925e701c8d23d026571a786557e85d8d02f20ac6c",
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_replay_evidence_shape_valid": (
        "8752d148716aef5907203611e368b60ac359122a1374f4f3ca6e53aa92d9d0c5",
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_terminal_disposition_v4_valid": (
        "0aba9d6d0764a5ce3a54ac3a81316d6f9100e90541e35773b91e991005ccc86e",
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_terminal_disposition_v5_valid": (
        "d141f2cbe8d1c037244258b3b4cbeac9a91def3ceee6d9195f67d1bf16458556",
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_terminal_disposition_valid": (
        "24c4a4bb920665a16e6f74a29b0479df4a6c0785ddb5a230663e782440f7a3a3",
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_eligible": (
        "d85161ad9c25758b6def57b222be66c39b715f88091f9776f79f2a4120f79344",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_evidence": (
        "3f7258913fbbbe0929a1ff2f47b911696cf41beb9f4a0c70cc374d3c5085faf7",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog", "TimeZone=UTC"),
    ),
    "provider_directory_terminal_root_retirement_marker_valid": (
        "6925f2f645f4d08b56d3cf29fa5a74359374e6beaa9c3a0cc747d297d145e497",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_relation_evidence": (
        "d30e84d4fcc4219d205214a80db974f7bd61abe01aeae21a4c5d931d52bd45f2",
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog", "TimeZone=UTC"),
    ),
    "provider_directory_terminal_root_retirement_v2_eligible": (
        "b4e89e81f328959d640dce18991ba4ddc1ab78ab7e8dcdf6b74cad69f5c30dd3",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_v2_evidence": (
        "101613f033f8b0629e2c2d135d67a2b39d800cdae7025366ee067a9a9faef308",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog", "TimeZone=UTC"),
    ),
    "provider_directory_terminal_root_retirement_v2_marker_valid": (
        "dcfca90509f87b52cf7de3e52e5c3e10a9ee74411dcb660b86146f72b29b3dcb",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_uhc_flex_practitioner_dataset_ready": (
        "4039dc1ed3a82ef468d0633c7db9901038bf7d213dca78c496fa1d913f34a15a",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_uhc_flex_practitioner_dataset_valid": (
        "d097aabe7f09ebbabb5eaa463a320a3a3c905914385b0f4af21c35aa5bf3df27",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
}


class FHIRSourceCustodyError(ValueError):
    """Retained source custody is unavailable or has changed."""


async def _require_routine_capabilities(connection, runtime_roles, schema, owner_oid):
    """Exclude directly callable definer capabilities in every non-system namespace."""
    unsafe = await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype)
        AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'
        AND (n.nspname<>$2 OR p.proowner<>$3::oid OR p.proname NOT IN
          ('provider_directory_uhc_flex_practitioner_dataset_ready','provider_directory_uhc_flex_practitioner_dataset_valid'))
        AND EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname=ANY($1::text[])
          AND has_schema_privilege(r.oid,n.oid,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')))""",
        list(runtime_roles),
        schema,
        owner_oid,
    )
    if unsafe:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


_FIRST_OWNER_LINEAGE_BODY_HASHES = {
    "provider_directory_terminal_root_retirement_evidence": "343b3eb1ab437f0b9402ea3633f49aff0831f3fd684c0ea82dc9b989c821663e",
    "provider_directory_terminal_root_retirement_eligible": "e580421dda5707c43a1ded063494ccb9f3197efa016b71ad1ff86ac47a80af92",
}


def _typed_retirement_digest(name, schema):
    """Keep retained readers compatible with only the exact source-reviewed casts."""
    from db.migration_cms_retirement_resolution import ORIGINAL_SHA256, retirement_resolution_bodies

    if name not in ORIGINAL_SHA256:
        return None
    hardened = retirement_resolution_bodies(schema)[name][1].replace(_identifier(schema) + ".", '"__schema__".')
    return hashlib.sha256(_normalized(hardened).encode()).hexdigest()


def _typed_terminal_digest(name, schema):
    """Accept the typed terminal guard only under its unchanged predecessor contract."""
    from db.migration_cms_terminal_resolution import GUARD, terminal_guard_bodies

    if name != GUARD:
        return None
    previous, hardened = terminal_guard_bodies(schema)
    bodies = [body.replace(_identifier(schema) + ".", '"__schema__".') for body in (previous, hardened)]
    if hashlib.sha256(_normalized(bodies[0]).encode()).hexdigest() != _REACHABLE_FUNCTIONS[GUARD][0]:
        return None
    return hashlib.sha256(_normalized(bodies[1]).encode()).hexdigest()


def _cms_lifecycle_digests(name, schema):
    """Bind exact typed/CMS lifecycle alternatives to both retained predecessor pins."""
    from db.migration_cms_noncurrent_seal import GUARD, _migration
    from db.migration_fhir_source_rollback import rollback_guard_bodies

    if name != GUARD:
        return ()
    compact = _migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    original = compact._function_body(compact._endpoint_guard_sql(schema, compact=True))
    bodies = [original, *rollback_guard_bodies(schema)]
    bodies = [body.replace(_identifier(schema) + ".", '"__schema__".') for body in bodies]
    predecessor = hashlib.sha256(_normalized(bodies[0]).encode()).hexdigest()
    if predecessor != _REACHABLE_FUNCTIONS[GUARD][0] or predecessor != _FUNCTION_HASHES[GUARD]:
        return ()
    return tuple(hashlib.sha256(_normalized(body).encode()).hexdigest() for body in bodies[1:])


def _require_function(function_record, schema, owner_oid):
    """Recognize only the actual source-rendered reachable routine contract."""
    expected = _REACHABLE_FUNCTIONS.get(function_record["proname"])
    body_digest = function_record.get("body_sha256")
    if body_digest is None:
        body = _normalized(function_record["prosrc"].replace(_identifier(schema) + ".", '"__schema__".'))
        body_digest = hashlib.sha256(body.encode()).hexdigest()
    actual = (
        body_digest,
        function_record["lanname"],
        function_record["provolatile"],
        function_record["prosecdef"],
        None if function_record["proconfig"] is None else tuple(function_record["proconfig"]),
    )
    typed_replay_digest = None
    if function_record["proname"] == "guard_pd_endpoint_dataset_subset_replay_evidence":
        from db.migration_cms_replay_resolution import replay_guard_bodies

        hardened = replay_guard_bodies(schema)[1].replace(_identifier(schema) + ".", '"__schema__".')
        typed_replay_digest = hashlib.sha256(_normalized(hardened).encode()).hexdigest()
    is_matching = (
        expected is not None
        and actual[1:4] == expected[1:4]
        and function_record["proconfig"]
        in approved_cms_routine_settings(None if expected[4] is None else list(expected[4]))
        and actual[0]
        in (
            expected[0],
            _FIRST_OWNER_LINEAGE_BODY_HASHES.get(function_record["proname"]),
            typed_replay_digest,
            _typed_retirement_digest(function_record["proname"], schema),
            _typed_terminal_digest(function_record["proname"], schema),
            *_cms_lifecycle_digests(function_record["proname"], schema),
        )
    )
    if not is_matching or function_record["proowner"] != owner_oid:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


def _require_guard(guard_record, schema, owner_oid, name, event_mask):
    """Bind a native trigger's complete invocation to its reviewed guard body."""
    if guard_record is None:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    function_name = _GUARD_FUNCTIONS[name]
    if function_name == "guard_tin_npi_connector_endpoint_dataset":
        _require_function(guard_record, schema, owner_oid)
    expected_arguments = b"raw\x00" if name == "provider_directory_endpoint_dataset_admission_raw_guard" else b""
    body = _normalized(guard_record["prosrc"].replace(_identifier(schema) + ".", '"__schema__".'))
    is_security_definer = function_name in {
        "guard_provider_directory_endpoint_dataset_admission_seal",
        "guard_tin_npi_connector_endpoint_dataset",
    }
    settings = None if function_name == "guard_cms_npd_relationship_truncate" else ["search_path=pg_catalog"]
    if (
        guard_record["tgtype"] != event_mask
        or guard_record["tgenabled"] not in {"O", "A"}
        or not guard_record["unconditional"]
        or guard_record["tgdeferrable"]
        or guard_record["tginitdeferred"]
        or tuple(guard_record["update_columns"]) != _UPDATE_COLUMNS.get(name, ())
        or (guard_record["tgoldtable"], guard_record["tgnewtable"]) != _TRANSITION_TABLES.get(name, (None, None))
        or guard_record["tgnargs"] != bool(expected_arguments)
        or bytes(guard_record["tgargs"]) != expected_arguments
        or guard_record["nspname"] != schema
        or guard_record["proname"] != function_name
        or guard_record["proowner"] != owner_oid
        or guard_record["pronargs"] != 0
        or guard_record["lanname"] != "plpgsql"
        or not guard_record["returns_trigger"]
        or guard_record["prosecdef"] is not is_security_definer
        or guard_record["proconfig"] not in approved_cms_routine_settings(settings)
        or hashlib.sha256(body.encode()).hexdigest()
        not in (_FUNCTION_HASHES[function_name], *_cms_lifecycle_digests(function_name, schema))
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


async def _generic_custody_family(connection, source_pin, owner_role, runtime_roles, source_policy):
    """Verify the Reader or exact bound publisher before checking child custody."""
    from process.provider_directory_generic_source_provision import verify_generic_source_read_family

    if source_policy is None:
        family_rows = await verify_generic_source_read_family(
            connection, source_pin.schema_name, owner_role, runtime_roles
        )
    else:
        from process.provider_directory_generic_source_policy import bound_publisher
        from process.provider_directory_generic_source_provision import verify_generic_source_family

        publisher = bound_publisher()
        if (
            publisher.connection is not connection
            or publisher.policy is not source_policy
            or (source_policy.schema, source_policy.owner_role, source_policy.runtime_roles)
            != (source_pin.schema_name, owner_role, runtime_roles)
        ):
            raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
        family_rows = await verify_generic_source_family(connection, source_policy)
    return family_rows


async def _generic_partitioned_custody(connection, source_pin, owner_role, runtime_roles, *, source_policy=None):
    """Authenticate every child and the exact published acquisition binding."""
    from db.plan_release_storage import storage_relation
    from process.provider_directory_generic_source_storage import (
        HEADER,
        RELATIONS,
        _json,
        _validate_binding,
    )

    family_rows = await _generic_custody_family(connection, source_pin, owner_role, runtime_roles, source_policy)
    header = await connection.fetchrow(
        f"SELECT endpoint_id,acquisition_root_run_id,candidate_storage_json FROM "
        f"{storage_relation(source_pin.schema_name, HEADER)} WHERE dataset_id=$1 AND endpoint_id=$2 AND dataset_hash=$3",
        source_pin.dataset_id,
        source_pin.endpoint_id,
        source_pin.dataset_sha256,
    )
    if header is None:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    binding = _json(header["candidate_storage_json"])
    if type(binding) is not dict or type(binding.get("identity")) is not dict:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    identity = binding["identity"]
    _validate_binding(binding, identity, phases=("published",))
    if (
        identity["dataset_id"] != source_pin.dataset_id
        or identity["endpoint_id"] != source_pin.endpoint_id
        or identity["acquisition_root_run_id"] != header["acquisition_root_run_id"]
        or identity["metadata"].get("source_ids") != ["cms-npd"]
        or identity["metadata"].get("source_release", {}).get("vector_sha256") != source_pin.release_id
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    by_name = {relation_by_field["relname"]: relation_by_field for relation_by_field in family_rows}
    for root in RELATIONS:
        # Named and stored legacy proofs both bind each published child by its recorded name.
        relation_by_field = by_name.get(binding["tables"][root]["name"])
        if relation_by_field is None or list(relation_by_field["parents"]) != [by_name[root]["oid"]]:
            raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    return {"binding": binding, "family": [dict(relation_by_field) for relation_by_field in family_rows]}
