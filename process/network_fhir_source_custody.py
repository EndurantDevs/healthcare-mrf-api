# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recognize retained CMS row custody without freezing other directory editions.

Consumers must read retained payload references, not mutable dataset projection
children. This receipt does not admit generic FHIR sources or a serving candidate.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass

import asyncpg

from process.network_address_projection import _identifier
from process.network_custom_address_source import _require_transaction
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_membership_writer_closure import _protected_owner
from process.provider_directory_cms_receipt_guard import _normalized
from process.provider_directory_cms_serving_coverage import _PROOF_VERSION

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
        "3e5b6e56434b575dbed134d24b05e8ab0abdfa44b7940db3fa306a81d0fddace",
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
        "1eb12057312878315448b44f6376f328318d18720277399ae8f7efdb266bbb96",
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_evidence": (
        "44df84acba4f184300af188a01711ef958664b54bee01454fddc5d1f896f641f",
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


@dataclass(frozen=True)
class RetainedCMSFHIRSourceCustody:
    """Bounded scalar and native catalog identity; contains no source documents."""

    source_pin: PinnedFHIRMembershipSource
    owner_role: str
    runtime_roles: tuple[str, ...]
    proof_sha256: str
    catalog_sha256: str


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _arguments(source_pin, owner_role, runtime_roles):
    if type(source_pin) is not PinnedFHIRMembershipSource or source_pin.source_id != "cms-npd":
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    _identifier(owner_role)
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 64
        or len(set(runtime_roles)) != len(runtime_roles)
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    for role in runtime_roles:
        _identifier(role)


async def _catalog(connection, source_pin, owner_role, runtime_roles):
    """Lock the closed input roster, then validate native owners and role capabilities."""
    namespace = _identifier(source_pin.schema_name)
    await connection.execute(
        "LOCK TABLE "
        + ",".join(f"{namespace}.{_identifier(name)}" for name in _TABLES)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    if not _protected_owner(owner):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    relations = await connection.fetch(
        """SELECT c.oid::bigint,c.relname,c.relowner::bigint,c.relkind,c.relpersistence,
      c.relrowsecurity,c.relforcerowsecurity,n.oid::bigint AS schema_oid,n.nspowner::bigint,
      EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
      (SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attgenerated,
        pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a
       LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
       WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)::text AS columns,
      (SELECT jsonb_agg(jsonb_build_array(k.oid,k.contype,pg_get_constraintdef(k.oid)) ORDER BY k.oid)
       FROM pg_constraint k WHERE k.conrelid=c.oid)::text AS constraints,
      (SELECT jsonb_agg(jsonb_build_array(i.indexrelid,i.indisvalid,i.indisready,pg_get_indexdef(i.indexrelid)) ORDER BY i.indexrelid)
       FROM pg_index i WHERE i.indrelid=c.oid)::text AS indexes
      FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid
      WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname""",
        source_pin.schema_name,
        list(_TABLES),
    )
    _require_relations(source_pin, owner, relations)
    await _roles(
        connection,
        relations[0]["schema_oid"],
        owner["oid"],
        runtime_roles,
        [relation_record["oid"] for relation_record in relations],
    )
    guards = await _guards(connection, source_pin.schema_name, owner["oid"], relations)
    await _require_routine_capabilities(connection, runtime_roles, source_pin.schema_name, owner["oid"])
    routines = await _routines(connection, source_pin.schema_name, relations[0]["schema_oid"], owner["oid"])
    return _digest(
        {
            "relations": [dict(relation_record) for relation_record in relations],
            "guards": guards,
            "routines": [dict(relation_record) for relation_record in routines],
            "owner_oid": owner["oid"],
        }
    )


async def _routines(connection, schema, schema_oid, owner_oid):
    """Require every reachable helper's initial contract before retaining catalog identity."""
    routines = await connection.fetch(
        """SELECT p.oid::bigint,p.proname,p.proowner::bigint,p.prosecdef,
      p.proconfig,p.provolatile::text,p.proacl::text,l.lanname,
      pg_get_function_identity_arguments(p.oid) AS arguments,
      encode(sha256(convert_to(btrim(regexp_replace(replace(p.prosrc,$2::text,'"__schema__".'),
        '[[:space:]]+',' ','g')),'UTF8')),'hex') AS body_sha256
      FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.pronamespace=$1::oid ORDER BY p.oid""",
        schema_oid,
        _identifier(schema) + ".",
    )
    for routine_record in routines:
        if routine_record["proname"] in _REACHABLE_FUNCTIONS:
            _require_function(routine_record, schema, owner_oid)
    if not set(_REACHABLE_FUNCTIONS) <= {routine_record["proname"] for routine_record in routines}:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    if len(routines) > 1024:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    return routines


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


def _require_relations(source_pin, owner, relations):
    """Require the exact persistent, protected input relation family."""
    if len(relations) != len(_TABLES) or any(
        relation_record["relowner"] != owner["oid"]
        or relation_record["nspowner"] != owner["oid"]
        or relation_record["relkind"] != b"r"
        or relation_record["relpersistence"] != b"p"
        or relation_record["relrowsecurity"]
        or relation_record["relforcerowsecurity"]
        or relation_record["inherited"]
        for relation_record in relations
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    if (
        next(
            relation_record["oid"]
            for relation_record in relations
            if relation_record["relname"] == "provider_directory_dataset_resource"
        )
        != source_pin.resource_table_oid
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


async def _roles(connection, schema_oid, owner_oid, names, oids):
    """Permit guarded row DML, while excluding native DDL and trigger bypass."""
    unsafe = await connection.fetchval(
        """WITH configured AS (
      SELECT r.* FROM unnest($1::text[]) name LEFT JOIN pg_roles r ON r.rolname=name
    ), acl AS (
      SELECT a.*,false AS relation FROM pg_namespace n CROSS JOIN LATERAL
        aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.oid=$2::oid
      UNION ALL SELECT a.*,true FROM pg_class c CROSS JOIN LATERAL
        aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=ANY($4::oid[])
      UNION ALL SELECT a.*,true FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a
        WHERE att.attrelid=ANY($4::oid[]) AND att.attnum>0 AND NOT att.attisdropped
    ) SELECT EXISTS(SELECT 1 FROM configured r WHERE r.oid IS NULL OR r.rolsuper OR r.rolcreaterole
      OR r.rolreplication OR r.rolbypassrls OR pg_has_role(r.oid,$3::oid,'MEMBER')
      OR pg_has_role(r.oid,$3::oid,'SET') OR has_parameter_privilege(r.oid,'session_replication_role','SET')
      OR EXISTS(SELECT 1 FROM pg_roles elevated WHERE (elevated.rolsuper OR elevated.rolcreaterole)
        AND pg_has_role(r.oid,elevated.oid,'MEMBER')))
      OR EXISTS(SELECT 1 FROM acl a WHERE a.grantee<>$3::oid AND
        (a.is_grantable OR a.grantee NOT IN (SELECT oid FROM configured)
         OR (NOT a.relation AND a.privilege_type<>'USAGE')
         OR (a.relation AND a.privilege_type NOT IN ('SELECT','INSERT','UPDATE','DELETE'))))
      OR EXISTS(SELECT 1 FROM configured r WHERE NOT has_schema_privilege(r.oid,$2::oid,'USAGE'))""",
        list(names),
        schema_oid,
        owner_oid,
        oids,
    )
    if unsafe:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


async def _guards(connection, schema, owner_oid, relations):
    """Recognize required existing guard bodies and retain every observed trigger identity."""
    guard_records = await connection.fetch(
        """SELECT c.relname,t.tgname,t.tgtype,t.tgenabled::text,t.tgnargs,
      t.tgqual IS NULL AS unconditional,t.tgoldtable,t.tgnewtable,t.tgattr::text,t.tgargs,
      t.tgdeferrable,t.tginitdeferred,ARRAY(SELECT a.attname::text
        FROM unnest(t.tgattr::smallint[]) WITH ORDINALITY watched(attnum,ordinal)
        JOIN pg_attribute a ON a.attrelid=t.tgrelid AND a.attnum=watched.attnum
        WHERE NOT a.attisdropped ORDER BY watched.ordinal) AS update_columns,
      t.tgfoid::bigint,p.proname,p.proowner::bigint,p.prosecdef,p.proconfig,p.prosrc,
      p.pronargs,p.provolatile::text,n.nspname,l.lanname,p.prorettype='trigger'::regtype AS returns_trigger
      FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_proc p ON p.oid=t.tgfoid
      JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
      WHERE t.tgrelid=ANY($1::oid[]) AND NOT t.tgisinternal ORDER BY c.relname,t.tgname""",
        [guard_record["oid"] for guard_record in relations],
    )
    if (
        len(guard_records) > 256
        or sum(len(guard_record["prosrc"].encode()) for guard_record in guard_records) > 4 * 1024 * 1024
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    guard_by_identity = {
        (guard_record["relname"], guard_record["tgname"]): guard_record for guard_record in guard_records
    }
    for guard_record in guard_records:
        _require_function(guard_record, schema, owner_oid)
    for table, required in _GUARDS.items():
        for name, event_mask in required.items():
            _require_guard(guard_by_identity.get((table, name)), schema, owner_oid, name, event_mask)
    return [
        {
            key: (hashlib.sha256(field_value.encode()).hexdigest() if key == "prosrc" else str(field_value))
            for key, field_value in dict(guard_record).items()
        }
        for guard_record in guard_records
    ]


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
    if expected is None or actual != expected or function_record["proowner"] != owner_oid:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


def _require_guard(guard_record, schema, owner_oid, name, event_mask):
    """Bind a native trigger's complete invocation to its reviewed guard body."""
    if guard_record is None:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    function_name = _GUARD_FUNCTIONS[name]
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
        or guard_record["proconfig"] != settings
        or hashlib.sha256(body.encode()).hexdigest() != _FUNCTION_HASHES[function_name]
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


async def _proof(connection, source_pin):
    """Read immutable admission scalars and validate complete source-scoped witnesses as sets."""
    namespace = _identifier(source_pin.schema_name)
    proof_record = await connection.fetchrow(
        f"""SELECT d.content_proof_admission_sha256 AS admission_sha256,
      d.publication_metadata_sha256 AS metadata_sha256,d.resource_count,c.relationship_count,
      d.publication_metadata_summary_json->'network_bindings' AS network_bindings
      FROM {namespace}.provider_directory_endpoint_dataset d
      JOIN {namespace}.provider_directory_cms_candidate_coverage c ON c.dataset_id=d.dataset_id
        AND c.endpoint_id=d.endpoint_id AND c.release_id=$4 AND c.dataset_hash=d.dataset_hash
        AND c.proof_version=$5 AND c.admission_sha256=d.content_proof_admission_sha256
        AND c.metadata_sha256=d.publication_metadata_sha256
      JOIN {namespace}.provider_directory_cms_npd_relationship_receipt r ON r.dataset_id=d.dataset_id
        AND r.release_id=$4 AND r.projection_contract='cms-npd-reference-ledger-v1'
        AND r.relationship_count=c.relationship_count
      JOIN {namespace}.provider_directory_cms_serving_coverage served ON served.dataset_id=d.dataset_id
        AND served.release_id=$4 AND served.dataset_hash=d.dataset_hash AND served.proof_version=$5
        AND served.published_at=d.published_at
      WHERE d.dataset_id=$1 AND d.endpoint_id=$2 AND d.dataset_hash=$3
        AND d.status IN ('published','superseded') AND d.published_at IS NOT NULL
        AND d.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
        AND d.publication_metadata_summary_json->'source_release'->>'vector_sha256'=$4
        AND d.content_proof_admission_version=1 AND d.content_proof_admission_kind='generic'
        AND d.content_proof_resource_types=ARRAY['Endpoint','HealthcareService','InsurancePlan','Location',
          'Organization','OrganizationAffiliation','Practitioner','PractitionerRole']::varchar[]""",
        source_pin.dataset_id,
        source_pin.endpoint_id,
        source_pin.dataset_sha256,
        source_pin.release_id,
        _PROOF_VERSION,
    )
    if proof_record is None:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    valid = await connection.fetchval(
        f"""SELECT
      (SELECT count(*) FROM {namespace}.provider_directory_dataset_resource WHERE dataset_id=$1)=$3
      AND (SELECT count(*) FROM {namespace}.provider_directory_cms_npd_relationship WHERE dataset_id=$1)=$4
      AND NOT EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r
        LEFT JOIN {namespace}.provider_directory_cms_npd_resource_witness w
          USING(dataset_id,resource_type,resource_id)
        WHERE r.dataset_id=$1 AND (w.source_id IS DISTINCT FROM 'cms-npd' OR w.release_id IS DISTINCT FROM $2
          OR w.normalized_payload_hash IS DISTINCT FROM r.payload_hash
          OR (r.acquired_resource_sha256 IS NOT NULL AND w.raw_payload_sha256 IS DISTINCT FROM r.acquired_resource_sha256)))
      AND NOT EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r
        JOIN {namespace}.provider_directory_cms_npd_resource_witness w USING(dataset_id,resource_type,resource_id)
        LEFT JOIN {namespace}.provider_directory_entity_release_evidence e ON e.source_id='cms-npd'
          AND e.resource_type=r.resource_type AND e.resource_id=r.resource_id AND e.release_id=$2
        LEFT JOIN {namespace}.provider_directory_entity_source_binding b ON b.source_id='cms-npd'
          AND b.resource_type=r.resource_type AND b.resource_id=r.resource_id
        WHERE r.dataset_id=$1 AND r.resource_type IN ('Organization','Location')
          AND (e.payload_sha256 IS DISTINCT FROM w.raw_payload_sha256 OR b.resource_id IS NULL))""",
        source_pin.dataset_id,
        source_pin.release_id,
        proof_record["resource_count"],
        proof_record["relationship_count"],
    )
    if valid is not True:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    return _digest(dict(proof_record))


async def capture_retained_cms_fhir_source_custody(connection, source_pin, *, owner_role, runtime_roles):
    """Capture only an actual protected CMS edition in the caller repeatable snapshot."""
    try:
        _arguments(source_pin, owner_role, runtime_roles)
        await _require_transaction(connection)
        async with asyncio.timeout(3):
            catalog = await _catalog(connection, source_pin, owner_role, runtime_roles)
            proof = await _proof(connection, source_pin)
        return RetainedCMSFHIRSourceCustody(source_pin, owner_role, runtime_roles, proof, catalog)
    except ValueError, TypeError, asyncpg.PostgresError, OSError, TimeoutError:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable") from None


async def require_retained_cms_fhir_source_custody(connection, custody):
    """Recheck the full retained receipt; this is a point-in-time proof, not a cache."""
    if type(custody) is not RetainedCMSFHIRSourceCustody:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    current = await capture_retained_cms_fhir_source_custody(
        connection, custody.source_pin, owner_role=custody.owner_role, runtime_roles=custody.runtime_roles
    )
    if current != custody:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    return current
