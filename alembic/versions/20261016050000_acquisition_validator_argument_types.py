# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind acquisition and candidate validators to their exact native argument types."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261016050000_acquisition_validator_argument_types"
down_revision = "20261016040000_source_validator_argument_types"
branch_labels = None
depends_on = None

# Only31 varchar/text and four unknown/regclass conversions change existing bodies.
_TEXT_REPAIRS = (
    (
        "guard_pd_uhc_flex_practitioner_acquisition",
        "pd_uhc_flex_practitioner_terminal_set_sha256",
        "NEW.acquisition_id",
    ),
    (
        "provider_directory_uhc_flex_practitioner_dataset_ready",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "header.dataset_id",
    ),
    (
        "guard_pd_uhc_flex_practitioner_dataset",
        "provider_directory_uhc_flex_practitioner_dataset_ready",
        "NEW.dataset_id",
    ),
    (
        "guard_pd_uhc_flex_practitioner_dataset",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "NEW.dataset_id",
    ),
    (
        "provider_directory_rooted_graph_sha256",
        "provider_directory_rooted_graph_edge_set_sha256",
        "header.acquisition_id",
    ),
    (
        "provider_directory_rooted_graph_sha256",
        "provider_directory_rooted_graph_resource_set_sha256",
        "header.acquisition_id",
    ),
    (
        "provider_directory_rooted_graph_sha256",
        "provider_directory_rooted_graph_terminal_set_sha256",
        "header.acquisition_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_rooted_graph_dataset_ready",
        "NEW.root_dataset_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_rooted_graph_edge_set_sha256",
        "NEW.acquisition_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_rooted_graph_request_failure_coverage",
        "NEW.acquisition_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_rooted_graph_resource_set_sha256",
        "NEW.acquisition_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_rooted_graph_sha256",
        "NEW.acquisition_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_rooted_graph_terminal_set_sha256",
        "NEW.acquisition_id",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "provider_directory_uhc_flex_practitioner_dataset_ready",
        "NEW.root_dataset_id",
    ),
    (
        "provider_directory_rooted_graph_official_lineage_current",
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "dataset_id",
    ),
    (
        "provider_directory_rooted_graph_official_lineage_current",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "legacy.dataset_id",
    ),
    (
        "provider_directory_rooted_graph_dataset_ready",
        "provider_directory_rooted_graph_dataset_valid",
        "header.dataset_id",
    ),
    (
        "guard_provider_directory_rooted_graph_dataset",
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "NEW.dataset_id",
    ),
    (
        "guard_provider_directory_rooted_graph_dataset",
        "provider_directory_rooted_graph_dataset_ready",
        "NEW.dataset_id",
    ),
    (
        "guard_provider_directory_rooted_graph_dataset",
        "provider_directory_rooted_graph_dataset_valid",
        "NEW.dataset_id",
    ),
    (
        "guard_provider_directory_exact_logical_current",
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "dataset.dataset_id",
    ),
    (
        "guard_provider_directory_exact_logical_current",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "dataset.dataset_id",
    ),
    (
        "guard_provider_directory_rooted_graph_single_root_admission",
        "provider_directory_rooted_graph_request_failure_coverage",
        "candidate.acquisition_id",
    ),
)

_HEADER_CALL_REPAIRS = (
    (
        "guard_provider_directory_endpoint_dataset_admission_seal",
        '"__schema__"."provider_directory_endpoint_dataset_admission_metadata_sha256"(\n'
        "                  NEW.publication_metadata_summary_json,\n"
        "                  NEW.content_proof_admission_version,\n"
        "                  NEW.content_proof_admission_kind::text,\n"
        "                  NEW.content_proof_admission_sha256,\n"
        "                  NEW.content_proof_resource_types\n"
        "              )",
        '"__schema__"."provider_directory_endpoint_dataset_admission_metadata_sha256"(\n'
        "                  NEW.publication_metadata_summary_json,\n"
        "                  NEW.content_proof_admission_version,\n"
        "                  NEW.content_proof_admission_kind::text,\n"
        "                  NEW.content_proof_admission_sha256::text,\n"
        "                  NEW.content_proof_resource_types\n"
        "              )",
        1,
    ),
    (
        "guard_tin_npi_connector_endpoint_dataset",
        '"__schema__"."provider_directory_endpoint_dataset_admission_metadata_sha256"(\n'
        "                    NEW.publication_metadata_summary_json,\n"
        "                    NEW.content_proof_admission_version,\n"
        "                    NEW.content_proof_admission_kind::text,\n"
        "                    NEW.content_proof_admission_sha256,\n"
        "                    NEW.content_proof_resource_types\n"
        "                 )",
        '"__schema__"."provider_directory_endpoint_dataset_admission_metadata_sha256"(\n'
        "                    NEW.publication_metadata_summary_json,\n"
        "                    NEW.content_proof_admission_version,\n"
        "                    NEW.content_proof_admission_kind::text,\n"
        "                    NEW.content_proof_admission_sha256::text,\n"
        "                    NEW.content_proof_resource_types\n"
        "                 )",
        1,
    ),
    (
        "guard_tin_npi_connector_endpoint_dataset",
        '"__schema__"."provider_directory_subset_completion_proof_pair_valid"(\n'
        "                    NEW.completion_proof_json,\n"
        "                    NEW.completion_proof_sha256\n"
        "                )",
        '"__schema__"."provider_directory_subset_completion_proof_pair_valid"(\n'
        "                    NEW.completion_proof_json,\n"
        "                    NEW.completion_proof_sha256::text\n"
        "                )",
        1,
    ),
    (
        "guard_tin_npi_connector_endpoint_dataset",
        '"__schema__"."provider_directory_subset_completion_proof_shape_valid"(\n'
        "                NEW.completion_proof_json,\n"
        "                NEW.dataset_hash,\n"
        "                NEW.resource_count\n"
        "           )",
        '"__schema__"."provider_directory_subset_completion_proof_shape_valid"(\n'
        "                NEW.completion_proof_json,\n"
        "                NEW.dataset_hash::text,\n"
        "                NEW.resource_count\n"
        "           )",
        1,
    ),
    (
        "guard_tin_npi_connector_endpoint_dataset",
        '"__schema__"."provider_directory_subset_content_proof_valid"(\n'
        "            (NEW.publication_metadata_json::jsonb -> 'provider_directory_content_proof_v1'),\n"
        "            NEW.dataset_id,\n"
        "            NEW.endpoint_id,\n"
        "            NEW.acquisition_root_run_id,\n"
        "            NEW.publication_metadata_json::jsonb -> 'source_ids',\n"
        "            NEW.publication_metadata_json::jsonb -> 'selected_resources',\n"
        "            NEW.dataset_hash,\n"
        "            NEW.resource_count,\n"
        "            NEW.completion_proof_json\n"
        "                -> 'dataset' -> 'resource_hashes',\n"
        "            NEW.completion_proof_json\n"
        "                -> 'dataset' -> 'resource_counts'\n"
        "        )",
        '"__schema__"."provider_directory_subset_content_proof_valid"(\n'
        "            (NEW.publication_metadata_json::jsonb -> 'provider_directory_content_proof_v1'),\n"
        "            NEW.dataset_id::text,\n"
        "            NEW.endpoint_id::text,\n"
        "            NEW.acquisition_root_run_id::text,\n"
        "            NEW.publication_metadata_json::jsonb -> 'source_ids',\n"
        "            NEW.publication_metadata_json::jsonb -> 'selected_resources',\n"
        "            NEW.dataset_hash::text,\n"
        "            NEW.resource_count,\n"
        "            NEW.completion_proof_json\n"
        "                -> 'dataset' -> 'resource_hashes',\n"
        "            NEW.completion_proof_json\n"
        "                -> 'dataset' -> 'resource_counts'\n"
        "        )",
        1,
    ),
)

_CONTRACTS = (
    (
        "guard_pd_uhc_flex_practitioner_acquisition",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "ffb4d9dc87bf6520d18aa2d5db61f046632cc3d7223f488fd9673efc199e212f",
        "4eb8f0c29c805901d33e0f85fd339dcf4897fa435b3d2c19224e4aad758eed98",
    ),
    (
        "guard_pd_uhc_flex_practitioner_dataset",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "5ebeb80f411f08734d5ce393e2ea98b47dc1ed04f240f77ef1e57425c7d74e47",
        "e62cc87cf575d90044d1fe7544ca39f9681eaf38c0d55577613ab1aac45d9c14",
    ),
    (
        "guard_provider_directory_exact_logical_current",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "7cf65fe7043f5ccaf72699c821b2f5dec93ab36d4ba4d95a08442cfcf84834d9",
        "8db884e15bfb0af083f8cd2e59f9ee27f0d4ddb5a4ce5eb00a2f57ea5036e972",
    ),
    (
        "guard_provider_directory_rooted_graph_acquisition",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "35fcb0fffc1f0c6ad0772dc9d6b4a56c16fa43bcce31d94f4e97d4ef1c6f3e80",
        "63a7b70d8ff66686418955af92106c2bc4f0a6d16e1f37892d135fec347185e7",
    ),
    (
        "guard_provider_directory_rooted_graph_dataset",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "b735744368fa07d2c28016595d864ba7555876a0f15b87aae2e28bf67c3dab2a",
        "a271f4519c77780593f0d78bf9595b3a624fadec503f32119336c6369bfc03fc",
    ),
    (
        "guard_provider_directory_rooted_graph_single_root_admission",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "653e53953a766cb5faf04f16cd59580daf5b062f2ebbb1ff0524485ca17467eb",
        "2182b75267c57b2295928549b8f97020ddabdbe270171761b719f5c84f562801",
    ),
    (
        "pd_uhc_flex_practitioner_terminal_set_sha256",
        "candidate_acquisition_id",
        "text",
        "text",
        "sql",
        "s",
        True,
        "s",
        True,
        False,
        "674514b98ee059f400bc2d8487e83793be34e14c18124968b4cd152a28e1077b",
        "674514b98ee059f400bc2d8487e83793be34e14c18124968b4cd152a28e1077b",
    ),
    (
        "provider_directory_fhir_request_failure_coverage_valid",
        "proof",
        "jsonb",
        "boolean",
        "plpgsql",
        "i",
        False,
        "u",
        False,
        False,
        "a830b78ee40f760e066aeaa15608bd057de4015db38cee3f335b82b8882eb108",
        "a830b78ee40f760e066aeaa15608bd057de4015db38cee3f335b82b8882eb108",
    ),
    (
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        False,
        "9fed3d575d7c41c67e54ea2d28871e19e1c355917f62ba3c8e5a83312e90c770",
        "9fed3d575d7c41c67e54ea2d28871e19e1c355917f62ba3c8e5a83312e90c770",
    ),
    (
        "provider_directory_rooted_graph_dataset_ready",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        False,
        "07055ed29a3c9df23ace6ee366c41ece9e5a772e6091cd9b15e3ca6d5a18aa4a",
        "4d3be6973b10d4c12ce3aa8d05d0f1e4130d583dda7a47a931f69311a19e14ef",
    ),
    (
        "provider_directory_rooted_graph_dataset_valid",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        False,
        "4d24867b29c594becd003a40608dd74ff40f1b591826a7b70e23f67c14675469",
        "4d24867b29c594becd003a40608dd74ff40f1b591826a7b70e23f67c14675469",
    ),
    (
        "provider_directory_rooted_graph_edge_set_sha256",
        "target_acquisition_id",
        "text",
        "text",
        "sql",
        "s",
        True,
        "s",
        True,
        False,
        "85e54e338be08bea6f337b5d53200f3af719447fbffbd80385a3048941e05db8",
        "85e54e338be08bea6f337b5d53200f3af719447fbffbd80385a3048941e05db8",
    ),
    (
        "provider_directory_rooted_graph_official_lineage_current",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        False,
        "725ee4cd532a792ab35969f4c245ffb8d06e76e346d963fd17d8ba715f5f68b7",
        "5ea4b915d3d47e73f674a9151d722e743bd6d58a99474644626f7213cdaf9bdd",
    ),
    (
        "provider_directory_rooted_graph_request_failure_coverage",
        "target_acquisition_id",
        "text",
        "jsonb",
        "plpgsql",
        "s",
        False,
        "u",
        True,
        False,
        "3631dc12bba26237cbf8bc59d85fd2e542771007dbb6e27927eee907ed2ce1f3",
        "3631dc12bba26237cbf8bc59d85fd2e542771007dbb6e27927eee907ed2ce1f3",
    ),
    (
        "provider_directory_rooted_graph_resource_set_sha256",
        "target_acquisition_id",
        "text",
        "text",
        "sql",
        "s",
        True,
        "s",
        True,
        False,
        "14fa9fa28cec67c1122fff1455fe6966056d6ac3e0c659a80bea3058b8739e7a",
        "14fa9fa28cec67c1122fff1455fe6966056d6ac3e0c659a80bea3058b8739e7a",
    ),
    (
        "provider_directory_rooted_graph_sha256",
        "target_acquisition_id",
        "text",
        "text",
        "sql",
        "s",
        True,
        "s",
        True,
        False,
        "2dc1b4643c43fb0b5f63a181304cdfb29c3af3c1883cfeea590c4d1b2cebd566",
        "838eeb76d213bd14c0dec9da7b0678ace24580dfec650bfc29529f3fa85d88a9",
    ),
    (
        "provider_directory_rooted_graph_terminal_set_sha256",
        "target_acquisition_id",
        "text",
        "text",
        "sql",
        "s",
        True,
        "s",
        True,
        False,
        "9a58451224bd9a857845ef0e177f3b30b2e98f99e9868fab088d6c07f464cc85",
        "9a58451224bd9a857845ef0e177f3b30b2e98f99e9868fab088d6c07f464cc85",
    ),
    (
        "provider_directory_uhc_flex_practitioner_dataset_ready",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        False,
        "3bfee515615123a656d88d01e28b1f985a5a33fdd3463079c2f8a5ba187e267b",
        "bed679bedad7ef5f030946cbd50aeb1e109563e192676df8eaa8e3ea489bf681",
    ),
    (
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        False,
        "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
        "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
    ),
    (
        "ptg_wave_canonical_json_ascii_v1",
        "payload",
        "jsonb",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        False,
        True,
        "f6170ea1c34b5db60a764d94e706bf8c2eda7be2effe10f6a61b572aa3180f1b",
        "f6170ea1c34b5db60a764d94e706bf8c2eda7be2effe10f6a61b572aa3180f1b",
    ),
    (
        "ptg_wave_json_ascii_text_v1",
        "value",
        "text",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        False,
        True,
        "a05048b7719553db41babb3b2197be64d15b2dcf851f6fc46d9ed2e0d36616cf",
        "a05048b7719553db41babb3b2197be64d15b2dcf851f6fc46d9ed2e0d36616cf",
    ),
    (
        "validate_provider_directory_rooted_graph_stage_checks",
        "stage,history",
        "regclass,regclass",
        "void",
        "plpgsql",
        "v",
        False,
        "u",
        False,
        False,
        "782bc71b200601688d085ceb8d1da90062dc38bd2c1150069bc524decdb00d6d",
        "782bc71b200601688d085ceb8d1da90062dc38bd2c1150069bc524decdb00d6d",
    ),
    (
        "validate_provider_directory_rooted_graph_witnesses",
        "resource_stage,edge_stage,expected",
        "regclass,regclass,@work",
        "bigint[]",
        "plpgsql",
        "v",
        False,
        "u",
        False,
        False,
        "45267a36f7407caf769c9e7e7b49aec2f7d7f18aaecce0bf106420b46c772514",
        "d73942623452dd5b0c119107b56b86c511c0a30a9e086534f84534c6f04ff376",
    ),
    (
        "validate_provider_directory_rooted_graph_work_stage",
        "p_acquisition,p_action",
        "text,text",
        "void",
        "plpgsql",
        "v",
        False,
        "u",
        False,
        False,
        "5f5711011495fd423118c061f9f373c2c7c5609eacc188d2331e3e44cfac8c5f",
        "b5718e700382733df7295db26d3c437026c668f8ce60b24ce0402c7b8f6cd4b2",
    ),
    (
        "ptg_wave_canonical_json_ascii_v1",
        "payload",
        "json",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        False,
        True,
        "9c90caae8cdb499de8f1ee0c445af77fdfa83e6fb6884b884be7e9cbfde6e6cd",
        "9c90caae8cdb499de8f1ee0c445af77fdfa83e6fb6884b884be7e9cbfde6e6cd",
    ),
    (
        "provider_directory_subset_payload_canonical_json",
        "candidate",
        "jsonb",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        True,
        False,
        "2654253caaf9ab6dc5e5ac7fd309f57f791aec4369e90ed29231ee19f7a69915",
        "2654253caaf9ab6dc5e5ac7fd309f57f791aec4369e90ed29231ee19f7a69915",
    ),
    (
        "provider_directory_subset_payload_sha256",
        "candidate",
        "jsonb",
        "text",
        "sql",
        "i",
        True,
        "s",
        True,
        False,
        "189f0fecf9020abde98282df2a3089bc19ad1ccc96f93553f3125497e4817271",
        "189f0fecf9020abde98282df2a3089bc19ad1ccc96f93553f3125497e4817271",
    ),
    (
        "provider_directory_subset_completion_proof_pair_valid",
        "candidate,candidate_sha256",
        "jsonb,text",
        "boolean",
        "sql",
        "i",
        True,
        "s",
        True,
        False,
        "52d0e8b812dc70ebf7b75bd94dd95b7ee99cbf63ff58f6e9cdeda6dcb1d59c69",
        "52d0e8b812dc70ebf7b75bd94dd95b7ee99cbf63ff58f6e9cdeda6dcb1d59c69",
    ),
    (
        "provider_directory_subset_completion_proof_shape_valid",
        "candidate,expected_dataset_hash,expected_resource_count",
        "jsonb,text,bigint",
        "boolean",
        "plpgsql",
        "i",
        True,
        "s",
        True,
        False,
        "505a38a4011eb99d7f24a8ea344d8433c1514fb3c71e3944b8718b9504ce24f4",
        "505a38a4011eb99d7f24a8ea344d8433c1514fb3c71e3944b8718b9504ce24f4",
    ),
    (
        "provider_directory_endpoint_dataset_admission_metadata_sha256",
        "metadata_summary,admission_version,admission_kind,proof_sha256,resource_types",
        "jsonb,smallint,text,text,varchar[]",
        "varchar",
        "sql",
        "i",
        True,
        "s",
        True,
        False,
        "57c2890fad111ab97f0ac62e64d0e66ab1685d424259bfe839b823bb1650f40f",
        "57c2890fad111ab97f0ac62e64d0e66ab1685d424259bfe839b823bb1650f40f",
    ),
    (
        "guard_provider_directory_endpoint_dataset_admission_seal",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "bcdd097f2463823d776f9d04ec7ff5a361637a93d1d7f87eafd4363a5c7e2207",
        "e0a5b67b7158326f456e340304c53514dcc45a220366fe6cdd6e9c64f4024311",
    ),
    (
        "guard_tin_npi_connector_endpoint_dataset",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        False,
        "f640d2a672ed753e35c9357be7cd2bf2a5e9283a1167abd2f58be1db016a8b79",
        "d9bacbb47e3d5a3cb296a6af04c27c8970aeddbb40f6c304d6cf2badc99ba2e9",
    ),
    (
        "provider_directory_subset_canonical_sha256",
        "candidate",
        "jsonb",
        "text",
        "sql",
        "i",
        True,
        "s",
        True,
        False,
        "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
        "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
    ),
    (
        "provider_directory_subset_completion_canonical_json",
        "candidate",
        "jsonb",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        True,
        False,
        "b6109ebe0d2c8a7bdaf383ab044cd90a4c98745b4ebc41bc1e077ddcdb763e96",
        "b6109ebe0d2c8a7bdaf383ab044cd90a4c98745b4ebc41bc1e077ddcdb763e96",
    ),
    (
        "provider_directory_subset_content_proof_valid",
        "candidate,expected_dataset_id,expected_endpoint_id,expected_root_run_id,expected_source_ids,expected_selected_resources,expected_dataset_hash,expected_resource_count,expected_resource_hashes,expected_resource_counts",
        "jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb",
        "boolean",
        "plpgsql",
        "i",
        True,
        "s",
        True,
        False,
        "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
        "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
    ),
)

_TYPE_OIDS = {
    "": "",
    "text": "25",
    "varchar": "1043",
    "varchar[]": "1015",
    "smallint": "21",
    "bigint": "20",
    "json": "114",
    "jsonb": "3802",
    "boolean": "16",
    "trigger": "2279",
    "regclass": "2205",
    "void": "2278",
    "bigint[]": "1016",
}
_TABLES = (
    "provider_directory_uhc_flex_practitioner_acquisition",
    "provider_directory_uhc_flex_practitioner_dataset",
    "provider_directory_rooted_graph_acquisition",
    "provider_directory_rooted_graph_work",
    "provider_directory_rooted_graph_resource",
    "provider_directory_rooted_graph_edge",
    "provider_directory_rooted_graph_dataset",
    "provider_directory_rooted_graph_twin_admission",
    "provider_directory_endpoint_dataset",
)


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _calls(schema):
    calls = tuple(
        (
            caller,
            storage_relation(schema, callee) + "(" + argument + ")",
            storage_relation(schema, callee) + "(" + argument + "::text)",
            1,
        )
        for caller, callee, argument in _TEXT_REPAIRS
    )
    check = storage_identifier(schema) + ".validate_provider_directory_rooted_graph_stage_checks"
    legacy = storage_identifier(schema) + ".provider_directory_rooted_graph_"
    work = "'pg_temp.pdrg_work_stage'"
    history = "'" + legacy + "work_legacy'"
    calls += (
        (
            "validate_provider_directory_rooted_graph_work_stage",
            check + "(\n        " + work + "," + history + ")",
            check + "(\n        " + work + "::regclass," + history + "::regclass)",
            1,
        ),
    )
    calls += tuple(
        (
            "validate_provider_directory_rooted_graph_witnesses",
            check + "(\n        " + kind + "_stage,'" + legacy + kind + "_legacy')",
            check + "(\n        " + kind + "_stage,'" + legacy + kind + "_legacy'::regclass)",
            1,
        )
        for kind in ("resource", "edge")
    )
    return calls + tuple(
        (
            caller,
            old.replace('"__schema__"', storage_identifier(schema)),
            new.replace('"__schema__"', storage_identifier(schema)),
            count,
        )
        for caller, old, new, count in _HEADER_CALL_REPAIRS
    )


def _contracts_sql(schema):
    contract_rows = []
    for (
        name,
        argument,
        kind,
        returns,
        language,
        volatility,
        strict,
        parallel,
        definer,
        extra_path,
        previous,
        current,
    ) in _CONTRACTS:
        type_parts = tuple(
            "to_regtype(" + _literal(storage_relation(schema, "provider_directory_rooted_graph_work")) + ")::oid::text"
            if part == "@work"
            else _literal(_TYPE_OIDS[part])
            for part in kind.split(",")
        )
        fields = (name, argument, _TYPE_OIDS[returns], language, volatility, parallel, previous, current)
        contract_rows.append(
            "("
            + ",".join(map(_literal, fields))
            + ",concat_ws(' ',"
            + ",".join(type_parts)
            + "),"
            + ",".join(str(flag).lower() for flag in (strict, definer, extra_path))
            + ")"
        )
    return (
        "(VALUES " + ",".join(contract_rows) + ") AS contracts("
        "name,argument,return_oid,language,volatility,parallel,previous_hash,next_hash,argument_oid,strict,definer,extra_path)"
    )


def _body_hash(schema, alias):
    normalized = "replace(replace(" + alias + ".prosrc," + _literal(storage_identifier(schema)) + ",'\"__schema__\"'),"
    normalized += _literal("'" + schema + "'") + "," + _literal("'__schema__'") + ")"
    return "encode(sha256(convert_to(" + normalized + ",'UTF8')),'hex')"


def _validation_sql(schema):
    return f"""FOR expected IN SELECT * FROM {_contracts_sql(schema)} LOOP
        SELECT * INTO STRICT original FROM pg_proc
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=expected.name
              AND proargtypes::text=expected.argument_oid;
        IF original.prokind<>'f' OR original.prorettype::text<>expected.return_oid
            OR original.prolang<>(SELECT oid FROM pg_language WHERE lanname=expected.language)
            OR original.pronargs<>(CASE WHEN expected.argument='' THEN 0
                ELSE cardinality(string_to_array(expected.argument,',')) END)
            OR original.proargnames IS DISTINCT FROM (CASE WHEN expected.argument='' THEN NULL::text[]
                ELSE string_to_array(expected.argument,',') END)
            OR original.pronargdefaults<>0 OR original.provariadic<>0
            OR original.proargmodes IS NOT NULL OR original.proallargtypes IS NOT NULL
            OR original.proargdefaults IS NOT NULL OR original.protrftypes IS NOT NULL
            OR original.probin IS NOT NULL OR original.prosqlbody IS NOT NULL OR original.prosupport<>0
            OR original.prosecdef<>expected.definer OR original.proleakproof OR original.proretset
            OR original.proisstrict<>expected."strict" OR original.procost<>100 OR original.prorows<>0
            OR original.provolatile::text<>expected.volatility OR original.proparallel::text<>expected.parallel
            OR original.proconfig IS DISTINCT FROM (CASE WHEN expected.extra_path
                THEN ARRAY['search_path=pg_catalog, '||quote_ident({_literal(schema)})]::text[]
                ELSE ARRAY['search_path=pg_catalog']::text[] END)
            OR NOT EXISTS(SELECT FROM pg_roles WHERE oid=original.proowner) THEN
            RAISE EXCEPTION 'acquisition_validator_definition_changed';
        END IF;
        body_hash:={_body_hash(schema, "original")};
        IF body_hash NOT IN (expected.previous_hash,expected.next_hash) THEN
            RAISE EXCEPTION 'acquisition_validator_body_changed';
        END IF;
    END LOOP;"""


def _replacement_sql(schema):
    repair_values = ",".join(
        "(" + ",".join(map(_literal, (name, old, new))) + "," + str(count) + ")"
        for name, old, new, count in _calls(schema)
    )
    return f"""FOR expected IN SELECT * FROM {_contracts_sql(schema)} LOOP
        SELECT * INTO STRICT original FROM pg_proc
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=expected.name
              AND proargtypes::text=expected.argument_oid;
        body_hash:={_body_hash(schema, "original")};
        replacement:=original.prosrc;
        definition:=pg_get_functiondef(original.oid);
        IF body_hash<>expected.next_hash THEN
            FOR repair IN SELECT * FROM (VALUES {repair_values}) AS repairs(name,previous_call,next_call,occurrences)
                WHERE name=expected.name LOOP
                IF (length(replacement)-length(replace(replacement,repair.previous_call,'')))
                    /length(repair.previous_call)<>repair.occurrences THEN
                    RAISE EXCEPTION 'acquisition_validator_dispatch_changed';
                END IF;
                replacement:=replace(replacement,repair.previous_call,repair.next_call);
                definition:=replace(definition,repair.previous_call,repair.next_call);
            END LOOP;
            EXECUTE definition;
        END IF;
        SELECT * INTO STRICT updated FROM pg_proc WHERE oid=original.oid;
        IF (to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')
            OR updated.prosrc IS DISTINCT FROM replacement
            OR {_body_hash(schema, "updated")}<>expected.next_hash THEN
            RAISE EXCEPTION 'acquisition_validator_replacement_changed';
        END IF;
    END LOOP;"""


def _catalog_sql(side):
    return f"""SELECT jsonb_agg(to_jsonb(p)-'prosrc' ORDER BY p.oid) INTO {side}_catalog
        FROM pg_proc p WHERE p.oid=ANY(function_oids);
    SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.oid),'[]'::jsonb) INTO {side}_hooks
        FROM pg_trigger t WHERE tgfoid=ANY(function_oids);
    SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY classid,objid,objsubid,refclassid,refobjid,refobjsubid,deptype),
        '[]'::jsonb) INTO {side}_dependencies FROM pg_depend d
        WHERE (classid='pg_proc'::regclass AND objid=ANY(function_oids))
           OR (refclassid='pg_proc'::regclass AND refobjid=ANY(function_oids));
    SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY dbid,classid,objid,objsubid,refclassid,refobjid,deptype),
        '[]'::jsonb) INTO {side}_shared FROM pg_shdepend d
        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND classid='pg_proc'::regclass AND objid=ANY(function_oids);"""


def _repair_sql(schema):
    storage_identifier(schema)
    names = "ARRAY[" + ",".join(_literal(name) for name in sorted({entry[0] for entry in _CONTRACTS})) + "]::text[]"
    work = _literal(storage_relation(schema, "provider_directory_rooted_graph_work"))
    return f"""DO $migration$
    DECLARE expected record; repair record; original pg_proc%ROWTYPE; updated pg_proc%ROWTYPE;
        body_hash text; replacement text; definition text; function_oids oid[];
        before_catalog jsonb; after_catalog jsonb; before_hooks jsonb; after_hooks jsonb;
        before_dependencies jsonb; after_dependencies jsonb; before_shared jsonb; after_shared jsonb;
    BEGIN
        IF (SELECT count(*) FROM pg_proc WHERE pronamespace=to_regnamespace({_literal(schema)})
            AND proname=ANY({names}))<>{len(_CONTRACTS)} THEN
            RAISE EXCEPTION 'acquisition_validator_function_set_changed';
        END IF;
        IF NOT EXISTS(SELECT FROM pg_class c JOIN pg_type t ON t.oid=c.reltype
            WHERE c.oid=to_regclass({work}) AND t.typrelid=c.oid AND t.typtype='c'
              AND t.oid=to_regtype({work})) THEN
            RAISE EXCEPTION 'acquisition_validator_work_type_changed';
        END IF;
        {_validation_sql(schema)}
        SELECT array_agg(p.oid ORDER BY p.oid) INTO function_oids FROM pg_proc p
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=ANY({names});
        {_catalog_sql("before")}
        {_replacement_sql(schema)}
        {_catalog_sql("after")}
        IF before_catalog IS DISTINCT FROM after_catalog OR before_hooks IS DISTINCT FROM after_hooks
            OR before_dependencies IS DISTINCT FROM after_dependencies OR before_shared IS DISTINCT FROM after_shared THEN
            RAISE EXCEPTION 'acquisition_validator_catalog_changed';
        END IF;
    END $migration$"""


def upgrade():
    """Preserve native identities and privileges while repairing only typed calls."""
    schema = plan_release_schema()
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL search_path=pg_catalog")
    op.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(schema, name) for name in _TABLES)
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(_repair_sql(schema))


def downgrade():
    """Never restore overload-sensitive dispatch."""
    raise RuntimeError("acquisition_validator_argument_types_downgrade_unsupported")
