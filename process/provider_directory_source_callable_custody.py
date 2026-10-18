# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact callable custody for the existing SOURCE acquisition and control protocol."""

import asyncio
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_cms_callable_custody import (
    _FUNCTION_SQL,
    _ROLE_GRAPH_SQL,
    _admin_identity,
    _body_digest,
    _decoded,
)
from process.provider_directory_cms_receipt_custody import _require_migration_identity
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_profile_initial_custody import _role_rows

# Existing trigger functions share native argument/return attributes; security remains explicit.
_GUARD_BODIES = {
    "account_provider_directory_rooted_graph_edge_budget()": (
        "dbbc9cf6fbfbdc1c69d35af758926e24eabd50e8e38bb4985f43f15eea79db79",
        True,
    ),
    "account_provider_directory_rooted_graph_resource_budget()": (
        "4c55848830478023a89c6cfeafbf0261e4ec63325d6dd0c7984db990f3bf952f",
        True,
    ),
    "account_provider_directory_rooted_graph_work_budget()": (
        "d79e8ff033754155abb1449ab1b3ca654165e9628ef2e7f56720e7ae08599429",
        True,
    ),
    "advance_reference_source_generation()": ("bc43708d0f74a9085b2a4ecfd2333ddec9eb4596f6cb1cb26fe56d3271db65a7", True),
    "guard_pd_dataset_bulk_parent_delete()": ("855e7bda72ceca29b68c5edab8cd198d35c1cdeaaed423ec68920ee03ed2c582", True),
    "guard_pd_dataset_migration_owner()": ("1961869026eea7109133e616c27af2d5017db3288646dfc331d9d948d640c2d8", False),
    "guard_pd_dataset_parent_key_update()": ("44c354bc8249e3ea1a28d31e05b2c960c467330bf95bc57325803e5ef4d2a324", True),
    "guard_pd_uhc_flex_candidate_insert()": ("72cc81c5ada8179ec25112abd7bac489e25cf38a2429456f191508ea6cc5bd39", False),
    "guard_pd_uhc_flex_practitioner_acquisition()": (
        "4eb8f0c29c805901d33e0f85fd339dcf4897fa435b3d2c19224e4aad758eed98",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_admission_immutable()": (
        "0e7a93349b6c4a7aa3bf20666e58aef0f48f800f9b5fde41411820ebc19d0ec4",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_admission_insert()": (
        "de2c9397143cfb2fd17b6341120898c07160931610870ccc534d0b2d928376d1",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_dataset()": (
        "e62cc87cf575d90044d1fe7544ca39f9681eaf38c0d55577613ab1aac45d9c14",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_dataset_endpoint()": (
        "4edc0f6abf9fbd66fe9ddc5f66f0a604b2bc6a90a54ae94b692025cab15f4e21",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_dataset_parent()": (
        "3c8f57a1907aea7861f696d1a3a545c32f2b1b0bd274c83b5e512aea7242d431",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_dataset_resource()": (
        "71a48605cdc23645ba5877576302c7dba600c1777d256fef5dce1ae7ccf2fc66",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_dataset_source()": (
        "d6c441ab63677f9253bac748293cf061ed43269813060a79cd2b87440131b6f5",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_resource()": (
        "20df0e7f1eb3f9b8c7bfe77ab5de8a9b042a314e135a82c2ffdce29010714c40",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_single_root_admission_insert()": (
        "8099c9027c6ac80fc6a2f0b98544e863a28eb9b030f61273f910192160f5a205",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_twin_attempt_immutable()": (
        "7065b5e7c39a2ae2c45f8d395077717dc5b99f26a3e5ffd80f6b11baaf72b3bd",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_twin_attempt_insert()": (
        "98442e617b1ca2820ae2afc3568092103040b1cfafd6c4c77642e8c24660bbcc",
        True,
    ),
    "guard_pd_uhc_flex_practitioner_work()": ("6a7ede60e65bcfe0a8b0371e757790036291d300c5d9c639092e1486f24475e6", True),
    "guard_provider_directory_endpoint_dataset_admission_seal()": (
        "e0a5b67b7158326f456e340304c53514dcc45a220366fe6cdd6e9c64f4024311",
        True,
    ),
    "guard_provider_directory_exact_logical_current()": (
        "8db884e15bfb0af083f8cd2e59f9ee27f0d4ddb5a4ce5eb00a2f57ea5036e972",
        True,
    ),
    "guard_provider_directory_reviewed_subset_activation_source()": (
        "198b932e871554ad324276d2b3c0f34982710d4dd685e120a33c6f51ad4331c3",
        True,
    ),
    "guard_provider_directory_rooted_graph_acquisition()": (
        "63a7b70d8ff66686418955af92106c2bc4f0a6d16e1f37892d135fec347185e7",
        True,
    ),
    "guard_provider_directory_rooted_graph_copy_owner()": (
        "10d3203c37c0ba2673ae6e9bc8faf50fdeda5a0f11b1d81f046f53ea49ebe570",
        False,
    ),
    "guard_provider_directory_rooted_graph_dataset()": (
        "a271f4519c77780593f0d78bf9595b3a624fadec503f32119336c6369bfc03fc",
        True,
    ),
    "guard_provider_directory_rooted_graph_dataset_resource()": (
        "15e4121640706463b6c94ad10087e5174b9ef6d2d38707e22e67145523e4b451",
        True,
    ),
    "guard_provider_directory_rooted_graph_dependency()": (
        "627aded81121718e2041ba80751bb08a5086503c91649947fa2e1a43f6f225a6",
        True,
    ),
    "guard_provider_directory_rooted_graph_single_root_admission()": (
        "2182b75267c57b2295928549b8f97020ddabdbe270171761b719f5c84f562801",
        True,
    ),
    "guard_provider_directory_rooted_graph_storage_seal()": (
        "85749d43b688bdc991c0d4a03ccc1ec9c4aedbd4360ea606b60dc66028283c75",
        False,
    ),
    "guard_provider_directory_rooted_graph_twin_admission()": (
        "8add8998abf9e3d18ab45f91cdbd1b2e512f205ff6694f84c54b97ef7f3dd4a5",
        True,
    ),
    "guard_provider_directory_rooted_graph_twin_attempt()": (
        "d2cd1d2117e1c2e384647d8bf4aae6e5926d6d1d2f3031f15ad865271144fbb2",
        True,
    ),
    "guard_provider_directory_rooted_graph_work()": (
        "9ef517da1a928a4e28868c74cff416d5545f23616eed948dd5bad503ce026043",
        True,
    ),
    "guard_provider_directory_subset_published_source()": (
        "19db33ae97daa0002477eb473e9e94abfae52fb8f1abd2ba7789f4c224ce4538",
        True,
    ),
    "guard_provider_directory_terminal_root_retirement_child()": (
        "fa6a7b4f6ee25122f74a5878d2508a6d0dba836c7ec284bdd902c1dbcea46dfb",
        True,
    ),
    "guard_provider_directory_uhc_flex_npi_cohort()": (
        "5d5d0a70c29af4d51b482a6239728db6cf11e2aaff063e5982e54ebfc31420e0",
        True,
    ),
    "guard_provider_directory_uhc_flex_npi_member()": (
        "71c03a1d052488201a0d6f816199cc4c12af865c009392aeb819ebae9ddde830",
        True,
    ),
    "guard_tin_npi_connector_endpoint_dataset()": (
        "d9bacbb47e3d5a3cb296a6af04c27c8970aeddbb40f6c304d6cf2badc99ba2e9",
        True,
    ),
    "pd_practitioner_migration_fence()": ("6f93e6db20a7cb5c3179c67a0802487765cff4b553d9cf236b85cab5b116f45f", False),
    "provider_directory_profile_delta_receipt_immutable_v1()": (
        "cdc932f14df3ded994c886d3a806d4dba781259914828831ff4aa002b766a7e3",
        False,
    ),
    "reject_provider_directory_rooted_graph_witness_mutation()": (
        "c735cd0801e1e9f025abbe66515dd2c7afde8a9ab3be8b52bad70359120a53b2",
        False,
    ),
    "validate_provider_directory_uhc_flex_npi_members()": (
        "76adc010e0f35636cf1c54242f0e50ecd5d7758116bd227ff2b4a21bca7ea433",
        True,
    ),
}
_CONTRACTS = {
    key: ((), "trigger", (), "plpgsql", "v", False, "u", security, digest)
    for key, (digest, security) in _GUARD_BODIES.items()
}
_CONTRACTS.update(
    {
        "admit_pd_uhc_flex_practitioner_stage(text,text,bigint,integer,text)": (
            (
                ("aid", "text"),
                ("cid", "text"),
                ("requested_npi", "bigint"),
                ("claim_attempt", "integer"),
                ("claim_token", "text"),
            ),
            "bigint",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "f62d840b601964acad148f42053c7b14bd40bdeb99b0905142527668cd71a0de",
        ),
        "admit_provider_directory_rooted_graph_witnesses(text,text,integer,text,regclass,regclass)": (
            (
                ("p_acquisition", "text"),
                ("p_query", "text"),
                ("p_attempt", "integer"),
                ("p_lease", "text"),
                ("resource_stage", "regclass"),
                ("edge_stage", "regclass"),
            ),
            "bigint[]",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "27f61dae296ee3622afe87b3730d75bc9368cbca1446faf4d985ad785d3d5ebc",
        ),
        "admit_provider_directory_rooted_graph_work(text,text)": (
            (("p_acquisition", "text"), ("p_action", "text")),
            "bigint",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "1d5455075987c37aec33e70ff7f422eb93e2a0d9b0711833af4d6b6596a9b6d0",
        ),
        "assert_provider_directory_rooted_graph_storage(text)": (
            (("p_acquisition", "text"),),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            False,
            "78c0a96d38c6290a88e52119d0e552dd7c3f29ec1f2a3137fd3f2c6d14ae4340",
        ),
        "attach_provider_directory_rooted_graph_storage(text)": (
            (("p_acquisition", "text"),),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "d0ca26c9d828d00487ba2f9a833cdd371e983c0310cc77efccfc78926558a88d",
        ),
        "finish_provider_directory_rooted_graph_initial_storage(text)": (
            (("p_acquisition", "text"),),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            False,
            "57f6990957c766007ad51d6b18113da5b027c979ed5596e4bc89499a1ce0846d",
        ),
        "finish_provider_directory_rooted_graph_storage(text)": (
            (("p_acquisition", "text"),),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "e14c3d9d31a9d851b1a0f4d5375640a1fedb8739c8f365e51aaa0cbaa628cb0f",
        ),
        "initialize_pd_uhc_flex_practitioner_work(text,text)": (
            (("aid", "text"), ("cid", "text")),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "788758b3dbc597a43997ec22044925dad3ef9b9d7e8a7f2db1c597e1f01fde42",
        ),
        "initialize_provider_directory_rooted_graph_work(text)": (
            (("p_acquisition", "text"),),
            "bigint",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "705420cf526b762d52a773fc46181910f9fdeb247211a6ab5699b1b6d3ebdb56",
        ),
        "pd_uhc_flex_practitioner_terminal_set_sha256(text)": (
            (("candidate_acquisition_id", "text"),),
            "text",
            (),
            "sql",
            "s",
            True,
            "s",
            True,
            "674514b98ee059f400bc2d8487e83793be34e14c18124968b4cd152a28e1077b",
        ),
        "prepare_pd_uhc_flex_practitioner_stage()": (
            (),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "2a23e58ebd49e83316e78f2592be4e602755ad1d122b57b9565fb9d4bb3e8729",
        ),
        "prepare_pd_uhc_flex_practitioner_work(text,text)": (
            (("aid", "text"), ("cid", "text")),
            "text",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "41d5431ab7236ad6e415190c7a3c889c2d655f1432c2e24f9e16834cdcecb4e9",
        ),
        "prepare_provider_directory_rooted_graph_storage(text)": (
            (("p_acquisition", "text"),),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "26fd54988de2a944bae396f137dc2b5623e922cf887a93d0aa7d568cb9ccfd49",
        ),
        "prepare_provider_directory_rooted_graph_witness_stage()": (
            (),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "3915aa7a323d9935af2ea52f76d54d98b43fcb5c13c84a1b2577d04cd38a2cdf",
        ),
        "prepare_provider_directory_rooted_graph_work_stage()": (
            (),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            True,
            "d60e39d1821337221ca10ead6e67c37fe2ccbbca6b2e31783ada52c5cd8931ff",
        ),
        "provider_directory_endpoint_dataset_admission_metadata_sha256(jsonb,smallint,text,text,varchar[])": (
            (
                ("metadata_summary", "jsonb"),
                ("admission_version", "smallint"),
                ("admission_kind", "text"),
                ("proof_sha256", "text"),
                ("resource_types", "varchar[]"),
            ),
            "varchar",
            (),
            "sql",
            "i",
            True,
            "s",
            True,
            "57c2890fad111ab97f0ac62e64d0e66ab1685d424259bfe839b823bb1650f40f",
        ),
        "provider_directory_fhir_request_failure_coverage_valid(jsonb)": (
            (("proof", "jsonb"),),
            "boolean",
            (),
            "plpgsql",
            "i",
            False,
            "u",
            False,
            "a830b78ee40f760e066aeaa15608bd057de4015db38cee3f335b82b8882eb108",
        ),
        "provider_directory_reviewed_subset_activation_valid(text)": (
            (("candidate_source_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "e45a0c892a551ef7081f6f8f9466c98cedb181037ec1ea455348229b2014ac64",
        ),
        "provider_directory_rooted_graph_dataset_intrinsic_valid(text)": (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "9fed3d575d7c41c67e54ea2d28871e19e1c355917f62ba3c8e5a83312e90c770",
        ),
        "provider_directory_rooted_graph_dataset_ready(text)": (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "4d3be6973b10d4c12ce3aa8d05d0f1e4130d583dda7a47a931f69311a19e14ef",
        ),
        "provider_directory_rooted_graph_dataset_valid(text)": (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "4d24867b29c594becd003a40608dd74ff40f1b591826a7b70e23f67c14675469",
        ),
        "provider_directory_rooted_graph_edge_set_sha256(text)": (
            (("target_acquisition_id", "text"),),
            "text",
            (),
            "sql",
            "s",
            True,
            "s",
            True,
            "85e54e338be08bea6f337b5d53200f3af719447fbffbd80385a3048941e05db8",
        ),
        "provider_directory_rooted_graph_official_lineage_current(text)": (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "5ea4b915d3d47e73f674a9151d722e743bd6d58a99474644626f7213cdaf9bdd",
        ),
        "provider_directory_rooted_graph_request_failure_coverage(text)": (
            (("target_acquisition_id", "text"),),
            "jsonb",
            (),
            "plpgsql",
            "s",
            False,
            "u",
            True,
            "3631dc12bba26237cbf8bc59d85fd2e542771007dbb6e27927eee907ed2ce1f3",
        ),
        "provider_directory_rooted_graph_resource_set_sha256(text)": (
            (("target_acquisition_id", "text"),),
            "text",
            (),
            "sql",
            "s",
            True,
            "s",
            True,
            "14fa9fa28cec67c1122fff1455fe6966056d6ac3e0c659a80bea3058b8739e7a",
        ),
        "provider_directory_rooted_graph_sha256(text)": (
            (("target_acquisition_id", "text"),),
            "text",
            (),
            "sql",
            "s",
            True,
            "s",
            True,
            "838eeb76d213bd14c0dec9da7b0678ace24580dfec650bfc29529f3fa85d88a9",
        ),
        "provider_directory_rooted_graph_terminal_set_sha256(text)": (
            (("target_acquisition_id", "text"),),
            "text",
            (),
            "sql",
            "s",
            True,
            "s",
            True,
            "9a58451224bd9a857845ef0e177f3b30b2e98f99e9868fab088d6c07f464cc85",
        ),
        "provider_directory_subset_canonical_sha256(jsonb)": (
            (("candidate", "jsonb"),),
            "text",
            (),
            "sql",
            "i",
            True,
            "s",
            True,
            "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
        ),
        "provider_directory_subset_completion_canonical_json(jsonb)": (
            (("candidate", "jsonb"),),
            "text",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            True,
            "b6109ebe0d2c8a7bdaf383ab044cd90a4c98745b4ebc41bc1e077ddcdb763e96",
        ),
        "provider_directory_subset_completion_proof_pair_valid(jsonb,text)": (
            (("candidate", "jsonb"), ("candidate_sha256", "text")),
            "boolean",
            (),
            "sql",
            "i",
            True,
            "s",
            True,
            "52d0e8b812dc70ebf7b75bd94dd95b7ee99cbf63ff58f6e9cdeda6dcb1d59c69",
        ),
        "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)": (
            (("candidate", "jsonb"), ("expected_dataset_hash", "text"), ("expected_resource_count", "bigint")),
            "boolean",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            True,
            "505a38a4011eb99d7f24a8ea344d8433c1514fb3c71e3944b8718b9504ce24f4",
        ),
        "provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)": (
            (
                ("candidate", "jsonb"),
                ("expected_dataset_id", "text"),
                ("expected_endpoint_id", "text"),
                ("expected_root_run_id", "text"),
                ("expected_source_ids", "jsonb"),
                ("expected_selected_resources", "jsonb"),
                ("expected_dataset_hash", "text"),
                ("expected_resource_count", "bigint"),
                ("expected_resource_hashes", "jsonb"),
                ("expected_resource_counts", "jsonb"),
            ),
            "boolean",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            True,
            "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
        ),
        "provider_directory_subset_payload_canonical_json(jsonb)": (
            (("candidate", "jsonb"),),
            "text",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            True,
            "2654253caaf9ab6dc5e5ac7fd309f57f791aec4369e90ed29231ee19f7a69915",
        ),
        "provider_directory_subset_payload_sha256(jsonb)": (
            (("candidate", "jsonb"),),
            "text",
            (),
            "sql",
            "i",
            True,
            "s",
            True,
            "189f0fecf9020abde98282df2a3089bc19ad1ccc96f93553f3125497e4817271",
        ),
        "provider_directory_uhc_flex_practitioner_dataset_ready(text)": (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "bed679bedad7ef5f030946cbd50aeb1e109563e192676df8eaa8e3ea489bf681",
        ),
        "provider_directory_uhc_flex_practitioner_dataset_valid(text)": (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
        ),
        "ptg_wave_canonical_json_ascii_v1(json)": (
            (("payload", "json"),),
            "text",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            False,
            "9c90caae8cdb499de8f1ee0c445af77fdfa83e6fb6884b884be7e9cbfde6e6cd",
        ),
        "ptg_wave_canonical_json_ascii_v1(jsonb)": (
            (("payload", "jsonb"),),
            "text",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            False,
            "f6170ea1c34b5db60a764d94e706bf8c2eda7be2effe10f6a61b572aa3180f1b",
        ),
        "ptg_wave_json_ascii_text_v1(text)": (
            (("value", "text"),),
            "text",
            (),
            "plpgsql",
            "i",
            True,
            "s",
            False,
            "a05048b7719553db41babb3b2197be64d15b2dcf851f6fc46d9ed2e0d36616cf",
        ),
        "public_evidence_npi_valid(text)": (
            (("candidate_npi", "text"),),
            "boolean",
            (),
            "sql",
            "i",
            True,
            "s",
            False,
            "861b34367a7671290c6464e1a7649f478dc3c31bf5f7c4b328d849d5cd3f8c44",
        ),
        "read_provider_directory_rooted_graph_initial_work(text,text,integer)": (
            (("p_acquisition", "text"), ("p_after", "text"), ("p_limit", "integer")),
            "record",
            (
                ("acquisition_id", "text"),
                ("scope_id", "text"),
                ("query_id", "text"),
                ("query_identity_sha256", "text"),
                ("query_identity_json_text", "text"),
                ("kind", "text"),
                ("resource_type", "text"),
                ("search_parameter", "text"),
                ("reference_type", "text"),
                ("reference_id", "text"),
                ("closure_scope", "text"),
                ("discovered_by_query_id", "text"),
                ("discovered_source_type", "text"),
                ("discovered_source_id", "text"),
                ("discovered_edge_sha256", "text"),
                ("status", "text"),
                ("attempt_count", "integer"),
                ("pagination_terminal", "boolean"),
            ),
            "sql",
            "s",
            False,
            "u",
            True,
            "31a4266b026957f738ceef8d5cb8f64d9c6e19762147e502fda4a5a28a3ad6e5",
        ),
        "validate_provider_directory_rooted_graph_stage_checks(regclass,regclass)": (
            (("stage", "regclass"), ("history", "regclass")),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            False,
            "782bc71b200601688d085ceb8d1da90062dc38bd2c1150069bc524decdb00d6d",
        ),
        "validate_provider_directory_rooted_graph_witnesses(regclass,regclass,@work)": (
            (("resource_stage", "regclass"), ("edge_stage", "regclass"), ("expected", "@work")),
            "bigint[]",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            False,
            "d73942623452dd5b0c119107b56b86c511c0a30a9e086534f84534c6f04ff376",
        ),
        "validate_provider_directory_rooted_graph_work_stage(text,text)": (
            (("p_acquisition", "text"), ("p_action", "text")),
            "void",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            False,
            "b5718e700382733df7295db26d3c437026c668f8ce60b24ce0402c7b8f6cd4b2",
        ),
    }
)

SOURCE_CALLABLE_NAMES = tuple(sorted({key.split("(", 1)[0] for key in _CONTRACTS}))
SOURCE_DEFINER_NAMES = tuple(sorted({key.split("(", 1)[0] for key, spec in _CONTRACTS.items() if spec[7]}))

_PURE_NAMES = {
    "public_evidence_npi_valid",
    "ptg_wave_canonical_json_ascii_v1",
    "ptg_wave_json_ascii_text_v1",
}
_PUBLIC_NAMES = {
    "provider_directory_uhc_flex_practitioner_dataset_valid",
    "provider_directory_uhc_flex_practitioner_dataset_ready",
    "provider_directory_profile_delta_receipt_immutable_v1",
    "ptg_wave_canonical_json_ascii_v1",
    "ptg_wave_json_ascii_text_v1",
}
_SOURCE_EXEC_NAMES = {
    "provider_directory_subset_canonical_sha256",
    "provider_directory_subset_content_proof_valid",
    "provider_directory_subset_completion_proof_pair_valid",
    "provider_directory_subset_completion_proof_shape_valid",
    "provider_directory_endpoint_dataset_admission_metadata_sha256",
}
_TYPE_OIDS = {
    "boolean": "16",
    "bigint": "20",
    "smallint": "21",
    "integer": "23",
    "text": "25",
    "json": "114",
    "bigint[]": "1016",
    "varchar": "1043",
    "varchar[]": "1015",
    "regclass": "2205",
    "record": "2249",
    "void": "2278",
    "trigger": "2279",
    "jsonb": "3802",
}
_IDENTITY_SQL = """SELECT session_user::text AS session,current_user::text AS current,
 pg_catalog.pg_backend_pid() AS backend,pg_catalog.pg_current_xact_id()::text AS xid,
 (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) AS administrator"""
_READ_IDENTITY_SQL = """SELECT session_user::text AS session,current_user::text AS current,
 pg_catalog.pg_backend_pid() AS backend,
 (SELECT virtualtransaction FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid()
  AND locktype='virtualxid' AND granted) AS xid,
 (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) AS administrator"""
_REACH_SQL = """SELECT jsonb_agg(jsonb_build_array(r.oid::bigint,r.rolsuper,
 pg_has_role(r.oid,$1::oid,'USAGE'),pg_has_role(r.oid,$1::oid,'SET'),
 pg_has_role(r.oid,$2::oid,'USAGE'),pg_has_role(r.oid,$2::oid,'SET')) ORDER BY r.oid)
 FROM pg_catalog.pg_roles r"""
_WORK_TYPE_SQL = """SELECT c.oid::bigint AS relation,c.relowner::bigint AS owner,
 c.relkind::text AS kind,t.oid::bigint AS oid,t.typtype::text AS category,
 t.typrelid::bigint AS relation_oid,t.typowner::bigint AS type_owner,
 t.typnamespace::bigint AS namespace,t.typelem::bigint AS element,t.typarray::bigint AS array,
 a.typelem::bigint AS array_element,a.typowner::bigint AS array_owner,
 a.typnamespace::bigint AS array_namespace FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_type t ON t.oid=c.reltype JOIN pg_catalog.pg_type a ON a.oid=t.typarray
 WHERE c.oid=pg_catalog.to_regclass($1)"""


def _require(condition, code):
    if not condition:
        raise ValueError("source_callable_" + code)


def source_callable_signatures(schema):
    """Return the fixed overload set; names and signatures alone never confer authority."""
    return tuple(
        storage_relation(schema, key.split("(", 1)[0])
        + "("
        + ",".join(
            storage_relation(schema, "provider_directory_rooted_graph_work") if kind == "@work" else kind
            for _, kind in spec[0]
        )
        + ")"
        for key, spec in _CONTRACTS.items()
    )


def _require_definition(entry, spec, schema, namespace, types, quoted_schema):
    arguments, returns, outputs, language, volatility, strict, parallel, security, digest = spec
    definition = entry["definition"]
    names = [name for name, _ in (*arguments, *outputs)] or None
    expected_by_field = dict(
        prokind="f",
        prorettype=types[returns],
        pronargs=len(arguments),
        proargtypes=[types[kind] for _, kind in arguments],
        proargnames=names,
        proargmodes=[*("i" for _ in arguments), *("t" for _ in outputs)] if outputs else None,
        proallargtypes=[types[kind] for _, kind in (*arguments, *outputs)] if outputs else None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=security,
        proleakproof=False,
        proisstrict=strict,
        proretset=bool(outputs),
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=1000 if outputs else 0,
        provolatile=volatility,
        proparallel=parallel,
        proconfig=None
        if definition["proname"] == "provider_directory_profile_delta_receipt_immutable_v1"
        else ["search_path=pg_catalog, " + quoted_schema]
        if definition["proname"].startswith("ptg_wave_")
        else ["search_path=pg_catalog"],
    )
    _require(
        int(definition["pronamespace"]) == namespace
        and entry["language"] == language
        and all(definition[field] == field_value for field, field_value in expected_by_field.items())
        and _body_digest(definition["prosrc"], schema) == digest,
        "definition_changed",
    )


def _require_acl(entry, roles, *, legacy):
    name, previous = entry["definition"]["proname"], int(entry["definition"]["proowner"])
    target = roles["namespace_owner"] if name in _PURE_NAMES else roles["owner"]
    _require(previous in ({roles["builder"], target} if legacy else {target}), "owner_changed")
    required_edges = {(previous, previous, "EXECUTE", False)}
    if name in _PUBLIC_NAMES:
        required_edges.add((0, previous, "EXECUTE", False))
    elif previous == target:
        required_edges.add((roles["builder"], target, "EXECUTE", False))
    allowed_edges = set(required_edges)
    if name in _SOURCE_EXEC_NAMES or name == "public_evidence_npi_valid":
        allowed_edges.add((roles["owner"], previous, "EXECUTE", False))
    # Explicit original caller grants may already exist before ownership transfer.
    allowed_edges.add((roles["builder"], previous, "EXECUTE", False))
    actual_edges = set(map(tuple, entry["acl"]))
    _require(len(actual_edges) == len(entry["acl"]) and required_edges <= actual_edges <= allowed_edges, "acl_changed")
    if not legacy and name == "public_evidence_npi_valid":
        _require((roles["owner"], target, "EXECUTE", False) in actual_edges, "npi_executor_missing")


def _require_owner_reach(rows, roles, role_ids):
    _role_rows(rows, role_ids, 6)
    for role, superuser, owner_usage, owner_set, namespace_usage, namespace_set in rows:
        if superuser:
            continue
        if role not in {roles["owner"], roles["publisher"], roles["migration"]}:
            _require(not (owner_usage or owner_set), "owner_authority")
        if role not in {roles["namespace_owner"], roles["migration"]}:
            _require(not (namespace_usage or namespace_set), "namespace_owner_authority")


async def _capture_roles(connection, policy, migration_role, namespace_owner_role):
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    graph = _decoded(await connection.fetchval(_ROLE_GRAPH_SQL))
    by_name = {role["rolname"]: int(role["oid"]) for role in graph["roles"]}
    _require(len(by_name) == len(graph["roles"]), "role_set_changed")
    _require(
        type(namespace_owner_role) is str
        and namespace_owner_role in by_name
        and namespace_owner_role not in {policy.owner_role, policy.publisher_role, *policy.runtime_roles},
        "namespace_owner_required",
    )
    storage_identifier(namespace_owner_role)
    namespace_role = next(role for role in graph["roles"] if role["rolname"] == namespace_owner_role)
    _require(
        not any(
            namespace_role[field]
            for field in ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        ),
        "namespace_owner_unprotected",
    )
    roles_by_kind = dict(
        owner=owner,
        publisher=by_name[policy.publisher_role],
        builder=by_name[policy.control_runtime_roles[0]],
        namespace_owner=by_name[namespace_owner_role],
    )
    roles_by_kind["migration"] = _require_migration_identity({"role_graph": graph}, policy, migration_role)
    _require(roles_by_kind["namespace_owner"] != roles_by_kind["migration"], "namespace_owner_required")
    namespace = await connection.fetchrow(
        "SELECT oid::bigint AS oid,nspowner::bigint AS owner,quote_ident(nspname) AS identifier "
        "FROM pg_namespace WHERE nspname=$1",
        policy.schema,
    )
    _require(
        namespace is not None and namespace["owner"] == roles_by_kind["namespace_owner"], "namespace_owner_changed"
    )
    reach = _decoded(await connection.fetchval(_REACH_SQL, owner, roles_by_kind["namespace_owner"]))
    _require_owner_reach(reach, roles_by_kind, set(by_name.values()))
    return graph, roles_by_kind, dict(namespace), reach


async def _capture_work_type(connection, policy, roles, namespace, legacy, *, reference=None):
    reference = (
        storage_relation(policy.schema, "provider_directory_rooted_graph_work") if reference is None else reference
    )
    work_by_field = await connection.fetchrow(_WORK_TYPE_SQL, reference)
    _require(work_by_field is not None, "work_type_changed")
    work_by_field = dict(work_by_field)
    _require(
        work_by_field["kind"] == "p"
        and work_by_field["category"] == "c"
        and work_by_field["relation"] == work_by_field["relation_oid"]
        and work_by_field["owner"] == work_by_field["type_owner"] == work_by_field["array_owner"]
        and work_by_field["owner"] in ({roles["builder"], roles["owner"]} if legacy else {roles["owner"]})
        and work_by_field["namespace"] == work_by_field["array_namespace"] == namespace["oid"]
        and work_by_field["element"] == 0
        and work_by_field["array_element"] == work_by_field["oid"],
        "work_type_changed",
    )
    return work_by_field


async def _capture_functions(connection, policy, graph, roles, namespace, work, legacy):
    signatures = source_callable_signatures(policy.schema)
    function_rows = await connection.fetch(
        _FUNCTION_SQL,
        list(signatures),
        policy.schema,
        sorted({key.split("(", 1)[0] for key in _CONTRACTS}),
        list(policy.runtime_roles),
    )
    return _verify_function_rows(function_rows, policy, graph, roles, namespace, work, legacy)


def _verify_function_rows(function_rows, policy, graph, roles, namespace, work, legacy):
    oid_by_type = dict(_TYPE_OIDS, **{"@work": str(work["oid"])})
    signatures = source_callable_signatures(policy.schema)
    _require(
        len(function_rows) == len(signatures)
        and {function_row["signature"] for function_row in function_rows} == set(signatures),
        "overloads_changed",
    )
    function_by_signature = {}
    for function_row in function_rows:
        function_by_field = {
            field: function_row[field] if field in {"signature", "language"} else _decoded(function_row[field])
            for field in function_row.keys()
        }
        index = signatures.index(function_by_field["signature"])
        key = tuple(_CONTRACTS)[index]
        _require_definition(
            function_by_field, _CONTRACTS[key], policy.schema, namespace["oid"], oid_by_type, namespace["identifier"]
        )
        _require_acl(function_by_field, roles, legacy=legacy)
        _role_rows(function_by_field["access"], {int(role["oid"]) for role in graph["roles"]}, 6)
        function_by_signature[key] = function_by_field
    return function_by_signature


async def capture_source_callable_custody(connection, policy, *, migration_role, namespace_owner_role, legacy=False):
    """Set-based callable proof only; relation and data custody belongs to the admin cut."""
    _require(
        type(policy) is GenericSourcePolicy
        and type(legacy) is bool
        and connection.is_in_transaction() is True
        and policy.control_runtime_roles,
        "policy_required",
    )
    task, identity = asyncio.current_task(), dict(await connection.fetchrow(_IDENTITY_SQL))
    _require_actor(identity, policy, legacy=legacy)
    graph, roles, namespace, reach = await _capture_roles(connection, policy, migration_role, namespace_owner_role)
    work = await _capture_work_type(connection, policy, roles, namespace, legacy)
    functions = await _capture_functions(connection, policy, graph, roles, namespace, work, legacy)
    _require(
        asyncio.current_task() is task and dict(await connection.fetchrow(_IDENTITY_SQL)) == identity,
        "transaction_changed",
    )
    return dict(
        schema=policy.schema,
        schema_oid=namespace["oid"],
        roles=roles,
        role_graph=graph,
        reach=reach,
        work_type=work,
        functions=functions,
    )


def _require_actor(identity, policy, *, legacy):
    original_admin = identity["administrator"] and identity["session"] == identity["current"]
    is_original_builder = identity["session"] == identity["current"] in policy.control_runtime_roles
    is_original_publisher = identity["session"] == policy.publisher_role and identity["current"] in {
        policy.publisher_role,
        policy.owner_role,
    }
    _require(
        original_admin if legacy else original_admin or is_original_builder or is_original_publisher, "actor_changed"
    )


def _expected_function_acl(entry, roles):
    previous, name = int(entry["definition"]["proowner"]), entry["definition"]["proname"]
    target = roles["namespace_owner"] if name in _PURE_NAMES else roles["owner"]
    expected_edges = {
        (target if grantee == previous else grantee, target if grantor == previous else grantor, privilege, grantable)
        for grantee, grantor, privilege, grantable in entry["acl"]
    }
    if previous != target and not any(grantee == 0 for grantee, *_ in expected_edges):
        expected_edges.add((roles["builder"], target, "EXECUTE", False))
    if name == "public_evidence_npi_valid":
        expected_edges.add((roles["owner"], target, "EXECUTE", False))
    return sorted(expected_edges)


def _require_execute_delta(previous, current, receipt):
    roles = receipt["roles"]
    reach_by_role = {row[0]: row for row in receipt["reach"]}
    before = _role_rows(previous["access"], reach_by_role.keys(), 6)
    after = _role_rows(current["access"], reach_by_role.keys(), 6)
    is_pure = current["definition"]["proname"] in _PURE_NAMES
    boundaries = {roles[name] for name in ("owner", "namespace_owner", "publisher", "migration")}
    for role, prior in before.items():
        actual = after[role]
        target_usage = reach_by_role[role][4 if is_pure else 2]
        additional_usage = current["definition"]["proname"] == "public_evidence_npi_valid" and reach_by_role[role][2]
        _require(prior[:2] == actual[:2], "role_classifier_changed")
        _require(actual[-1] == (prior[-1] or target_usage or additional_usage), "execute_changed")
        if prior[1] and role not in boundaries:
            _require(actual[-1] == prior[-1], "ordinary_execute_changed")


def require_source_callable_delta(before, after):
    """Only declared ownership, owner ACL rewrites and the fixed NPI executor grant may change."""
    _require(
        {key: value for key, value in before.items() if key not in {"functions", "work_type"}}
        == {key: value for key, value in after.items() if key not in {"functions", "work_type"}},
        "adoption_scope_changed",
    )
    expected_work_by_field = dict(before["work_type"])
    for key in ("owner", "type_owner", "array_owner"):
        expected_work_by_field[key] = after["roles"]["owner"]
    _require(expected_work_by_field == after["work_type"], "adoption_type_changed")
    _require(before["functions"].keys() == after["functions"].keys() == _CONTRACTS.keys(), "adoption_set_changed")
    for key, previous in before["functions"].items():
        current = after["functions"][key]
        expected = deepcopy(previous["definition"])
        name = expected["proname"]
        target = after["roles"]["namespace_owner"] if name in _PURE_NAMES else after["roles"]["owner"]
        expected["proowner"] = str(target)
        expected["proacl"] = current["definition"]["proacl"]
        _require(
            expected == current["definition"] and previous["signature"] == current["signature"],
            "adoption_definition_changed",
        )
        _require(
            _expected_function_acl(previous, after["roles"]) == sorted(map(tuple, current["acl"])),
            "adoption_acl_changed",
        )
        _require_execute_delta(previous, current, after)


async def adopt_source_callable_custody(connection, policy, before):
    """Internal fixed fragment; the original admin operator owns verification, savepoint and CAS."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and before["schema"] == policy.schema,
        "explicit_admin_required",
    )
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    name_by_oid = {int(role["oid"]): role["rolname"] for role in before["role_graph"]["roles"]}
    for entry in sorted(before["functions"].values(), key=lambda value: int(value["definition"]["oid"])):
        previous, name = int(entry["definition"]["proowner"]), entry["definition"]["proname"]
        target_owner = before["roles"]["namespace_owner"] if name in _PURE_NAMES else before["roles"]["owner"]
        if previous != target_owner:
            await connection.execute(
                f"ALTER FUNCTION {entry['signature']} OWNER TO {storage_identifier(name_by_oid[target_owner])}"
            )
        if previous != target_owner and not any(grantee == 0 for grantee, *_ in entry["acl"]):
            await connection.execute(
                f"GRANT EXECUTE ON FUNCTION {entry['signature']} TO "
                + storage_identifier(name_by_oid[before["roles"]["builder"]])
            )
        if name == "public_evidence_npi_valid":
            owner_execute = next(
                access_row[-1] for access_row in entry["access"] if access_row[0] == before["roles"]["owner"]
            )
            if not owner_execute:
                await connection.execute(f"SET LOCAL ROLE {storage_identifier(name_by_oid[target_owner])}")
                await connection.execute(
                    f"GRANT EXECUTE ON FUNCTION {entry['signature']} TO " + storage_identifier(policy.owner_role)
                )
                await connection.execute("RESET ROLE")
    _require(
        asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
        "admin_transaction_changed",
    )


async def _capture_callable_capability_receipt(connection, policy, *, migration_role, namespace_owner_role, legacy):
    from process.provider_directory_native_counter_catalog import _counter_callable_capability

    _require(
        type(policy) is GenericSourcePolicy
        and type(legacy) is bool
        and connection.is_in_transaction() is True
        and policy.control_runtime_roles,
        "policy_required",
    )
    task, identity = asyncio.current_task(), dict(await connection.fetchrow(_IDENTITY_SQL))
    receipt = await capture_source_callable_custody(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role, legacy=legacy
    )
    namespace_by_field = dict(oid=receipt["schema_oid"], identifier=storage_identifier(policy.schema))
    native_counter = await _counter_callable_capability(
        connection, policy, receipt["roles"], namespace_by_field, receipt["role_graph"]
    )
    _require(
        asyncio.current_task() is task and dict(await connection.fetchrow(_IDENTITY_SQL)) == identity,
        "transaction_changed",
    )
    return receipt, native_counter


async def capture_source_callable_adoption_capability(connection, policy, *, migration_role, namespace_owner_role):
    """Explicit independent-admin pre-cut proof; only already-protected definers are allowed."""
    receipt, native_counter = await _capture_callable_capability_receipt(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role, legacy=True
    )
    return tuple(
        sorted(
            (
                *native_counter,
                *(
                    int(entry["definition"]["oid"])
                    for entry in receipt["functions"].values()
                    if entry["definition"]["prosecdef"]
                    and int(entry["definition"]["proowner"]) == receipt["roles"]["owner"]
                ),
            )
        )
    )


async def capture_source_callable_capability(connection, policy, *, migration_role, namespace_owner_role):
    """Authenticate the fixed completed callable contract on this original transaction."""
    receipt, native_counter = await _capture_callable_capability_receipt(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role, legacy=False
    )
    return tuple(
        sorted(
            (
                *native_counter,
                *(
                    int(entry["definition"]["oid"])
                    for entry in receipt["functions"].values()
                    if entry["definition"]["prosecdef"]
                ),
            )
        )
    )


async def _capture_catalog_work(connection, policy, roles, namespace):
    name = "provider_directory_rooted_graph_work"
    reference = await connection.fetchval(
        "SELECT c.oid::text FROM pg_catalog.pg_class c WHERE c.relnamespace=$1::oid AND c.relname=$2",
        namespace["oid"],
        name,
    )
    _require(
        type(reference) is str and reference.isascii() and reference.isdecimal() and int(reference) > 0,
        "work_type_changed",
    )
    work = await _capture_work_type(connection, policy, roles, namespace, False, reference=reference)
    _require(
        work["relation"] == int(reference)
        and await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_catalog.pg_class WHERE oid=$1::oid AND relnamespace=$2::oid AND relname=$3)",
            int(reference),
            namespace["oid"],
            name,
        )
        is True,
        "work_type_changed",
    )
    return work


def _catalog_function_references(rows, policy, namespace, work):
    oid_by_type = dict(_TYPE_OIDS, **{"@work": str(work["oid"])})
    signature_by_identity = {
        (key.split("(", 1)[0], tuple(oid_by_type[kind] for _, kind in spec[0])): signature
        for (key, spec), signature in zip(_CONTRACTS.items(), source_callable_signatures(policy.schema), strict=True)
    }
    _require(len(rows) == len(signature_by_identity), "overloads_changed")
    signature_by_oid = {}
    for row in rows:
        identity = row["name"], tuple(str(value) for value in row["arguments"])
        _require(
            type(row["oid"]) is int
            and 0 < row["oid"] < 2**32
            and row["namespace"] == namespace["oid"]
            and identity in signature_by_identity,
            "overloads_changed",
        )
        reference = str(row["oid"])
        _require(
            reference not in signature_by_oid and signature_by_identity[identity] not in signature_by_oid.values(),
            "overloads_changed",
        )
        signature_by_oid[reference] = signature_by_identity[identity]
    return signature_by_oid


def _normalize_catalog_functions(rows, references, policy, namespace, work):
    oid_by_type = dict(_TYPE_OIDS, **{"@work": str(work["oid"])})
    contract_by_signature = dict(zip(source_callable_signatures(policy.schema), _CONTRACTS.items(), strict=True))
    normalized_rows = []
    for row in rows:
        reference = row["signature"]
        _require(reference in references, "overloads_changed")
        signature = references[reference]
        key, spec = contract_by_signature[signature]
        definition = _decoded(row["definition"])
        _require(
            str(definition["oid"]) == reference
            and definition["proname"] == key.split("(", 1)[0]
            and int(definition["pronamespace"]) == namespace["oid"]
            and definition["proargtypes"] == [oid_by_type[kind] for _, kind in spec[0]],
            "overloads_changed",
        )
        normalized_rows.append(dict(row, signature=signature))
    return normalized_rows


async def _capture_catalog_functions(connection, policy, graph, roles, namespace, work):
    names = sorted({key.split("(", 1)[0] for key in _CONTRACTS})
    census = await connection.fetch(
        "SELECT p.oid::bigint AS oid,p.pronamespace::bigint AS namespace,p.proname AS name,"
        "p.proargtypes::oid[] AS arguments FROM pg_catalog.pg_proc p "
        "WHERE p.pronamespace=$1::oid AND p.proname=ANY($2::text[]) ORDER BY p.oid",
        namespace["oid"],
        names,
    )
    references = _catalog_function_references(census, policy, namespace, work)
    rows = await connection.fetch(
        _FUNCTION_SQL, list(references), str(namespace["oid"]), names, list(policy.runtime_roles)
    )
    normalized = _normalize_catalog_functions(rows, references, policy, namespace, work)
    return _verify_function_rows(normalized, policy, graph, roles, namespace, work, False)


async def attest_source_callable_capability(connection, policy, *, migration_role, namespace_owner_role):
    """Read the completed contract without admitting an actor or granting authority."""
    _require(
        type(policy) is GenericSourcePolicy and connection.is_in_transaction() is True and policy.control_runtime_roles,
        "policy_required",
    )
    task, identity = asyncio.current_task(), dict(await connection.fetchrow(_READ_IDENTITY_SQL))
    _require(type(identity["xid"]) is str and bool(identity["xid"]), "transaction_required")
    graph, roles, namespace, _reach = await _capture_roles(connection, policy, migration_role, namespace_owner_role)
    work = await _capture_catalog_work(connection, policy, roles, namespace)
    functions = await _capture_catalog_functions(connection, policy, graph, roles, namespace, work)
    from process.provider_directory_native_counter_catalog import _counter_callable_capability

    native_counter = await _counter_callable_capability(connection, policy, roles, namespace, graph)
    _require(
        asyncio.current_task() is task and dict(await connection.fetchrow(_READ_IDENTITY_SQL)) == identity,
        "transaction_changed",
    )
    return tuple(
        sorted(
            (
                *native_counter,
                *(int(entry["definition"]["oid"]) for entry in functions.values() if entry["definition"]["prosecdef"]),
            )
        )
    )
