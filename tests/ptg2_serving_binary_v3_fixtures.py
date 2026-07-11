# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small publish summaries shared by PTG2 v3 tests."""

from process.ptg_parts import ptg2_serving_binary as serving_binary


def _provider_code_summary():
    return {
        "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
        "row_count": 3,
        "pair_count": 3,
        "code_count": 3,
        "duplicate_pair_count": 0,
        "provider_set_count": 2,
    }


def _forward_page_summary():
    return {
        "artifact_kind": serving_binary.PTG2_SERVING_BINARY_BY_CODE_PAGE_V3_KIND,
        "page_rows": 64,
        "row_count": 4,
        "code_count": 3,
        "block_count": 3,
        "copy_record_count": 3,
        "storage": {
            "record_count": 3,
            "entry_count": 4,
            "stored_payload_bytes": 48,
            "raw_payload_bytes": 48,
        },
    }


def _provider_page_summary():
    return {
        "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND,
        "block_span": 1,
        "page_rows": 64,
        "row_count": 4,
        "source_row_count": 4,
        "provider_set_count": 2,
        "truncated_provider_set_count": 0,
        "block_count": 1,
        "copy_record_count": 1,
        "storage": {
            "record_count": 1,
            "entry_count": 2,
            "stored_payload_bytes": 64,
            "raw_payload_bytes": 64,
        },
    }


def stream_summary_by_kind():
    return {
        serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
            "row_count": 4,
            "group_count": 3,
            "code_count": 3,
            "price_set_count": 2,
            "maximum_price_key": 1,
            "price_key_upper_bound": 2,
            "provider_set_count": 2,
            "provider_set_codes": _provider_code_summary(),
            "by_code_page": _forward_page_summary(),
            "provider_set_page": _provider_page_summary(),
        },
        serving_binary.PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
            "row_count": 2,
            "price_set_count": 2,
            "id_bytes": 16,
        },
        serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
            "row_count": 3,
            "atom_reference_count": 3,
            "price_set_count": 2,
            "maximum_price_key": 1,
            "atom_key_bits": 24,
            "atom_key_bytes": 3,
        },
        serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
            "atom_count": 2,
            "attribute_count": 7,
            "atom_key_bits": 24,
            "atom_key_bytes": 3,
        },
    }
