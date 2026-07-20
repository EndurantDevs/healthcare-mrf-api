# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_shared_blocks import SharedBlock, shared_support_digest
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _physical_serving_index,
    _shared_layout_support_digest,
)


def _serving_index_by_field(*, full_rebuild_scope_digest=None):
    """Build a minimal validated serving index for rebuild identity assertions."""

    finalizer_summary_by_field = {
        "blocks": {
            "price_dictionary_encoder": {"codec": "test"},
            "assigned_encoder": {"version": 1},
        },
        "dense_keys": {"price": {"count": 3}},
        "preservation": {"encoded_records": 5},
        "source_count": 1,
        "timings": {"finalizer_seconds": 0.5},
    }
    stream_summary_by_name = {
        "price_set_atom_memberships_v3": {"block_count": 1},
        "price_atoms_v3": {"block_count": 1},
    }
    return _physical_serving_index(
        snapshot_key=17,
        coverage_scope_id=b"c" * 32,
        finalizer_summary=finalizer_summary_by_field,
        price_publication=SimpleNamespace(
            stream_summaries=stream_summary_by_name,
            atom_key_bits=32,
            price_atom_constant_keys={"negotiated_type": 0},
            price_atom_constant_values={"negotiated_type": "negotiated"},
            stage_metrics={"row_count": 3},
        ),
        graph_publication=SimpleNamespace(
            owner_count=1,
            provider_group_count=2,
            npi_count=3,
            block_count=4,
        ),
        code_count=6,
        audit_sample={"sample_digest": "a" * 64},
        source_witness={"witness_digest": "b" * 64},
        provider_identifier_quarantine=provider_identifier_quarantine_payload({}),
        stored_byte_count=7,
        full_rebuild_scope_digest=full_rebuild_scope_digest,
    )


def _support_components_by_field():
    """Return independent canonical maps used by the final support digest."""

    return (
        {"contract_version": 1, "dictionary": "a" * 64},
        {"sample_digest": "b" * 64},
        {"witness_digest": "c" * 64},
    )


def _content_address(block_key):
    return SharedBlock(
        object_kind="test_content_v1",
        block_key=block_key,
        fragment_no=0,
        entry_count=1,
        codec="none",
        raw_byte_count=7,
        payload=b"payload",
    ).block_hash


def test_serving_index_persists_only_present_full_rebuild_scope_digest():
    """Persist the opaque scope marker without changing the legacy index schema."""

    legacy_index_by_field = _serving_index_by_field()
    scoped_index_by_field = _serving_index_by_field(
        full_rebuild_scope_digest=" A" + "B" * 63 + " "
    )
    scoped_without_marker_by_field = {
        key: value
        for key, value in scoped_index_by_field.items()
        if key != "full_rebuild_scope_digest"
    }

    assert "full_rebuild_scope_digest" not in legacy_index_by_field
    assert scoped_index_by_field["full_rebuild_scope_digest"] == "a" + "b" * 63
    assert scoped_without_marker_by_field == legacy_index_by_field


def test_full_rebuild_scope_salts_only_final_layout_support_identity():
    """Scope layout support identity while preserving the exact legacy digest."""

    (
        core_support_by_field,
        audit_sample_by_field,
        source_witness_by_field,
    ) = _support_components_by_field()
    legacy_support_by_field = {
        **core_support_by_field,
        "audit_sample": audit_sample_by_field,
        "source_witness": source_witness_by_field,
    }
    digest_by_name = {
        "legacy": _shared_layout_support_digest(
            core_support=core_support_by_field,
            audit_sample=audit_sample_by_field,
            source_witness=source_witness_by_field,
        ),
        "first_scope": _shared_layout_support_digest(
            core_support=core_support_by_field,
            audit_sample=audit_sample_by_field,
            source_witness=source_witness_by_field,
            full_rebuild_scope_digest="1" * 64,
        ),
        "same_scope": _shared_layout_support_digest(
            core_support=core_support_by_field,
            audit_sample=audit_sample_by_field,
            source_witness=source_witness_by_field,
            full_rebuild_scope_digest="1" * 64,
        ),
        "other_scope": _shared_layout_support_digest(
            core_support=core_support_by_field,
            audit_sample=audit_sample_by_field,
            source_witness=source_witness_by_field,
            full_rebuild_scope_digest="2" * 64,
        ),
    }

    assert digest_by_name["legacy"] == shared_support_digest(legacy_support_by_field)
    assert digest_by_name["first_scope"] == digest_by_name["same_scope"]
    assert digest_by_name["first_scope"] != digest_by_name["other_scope"]
    assert digest_by_name["first_scope"] != digest_by_name["legacy"]
    assert _support_components_by_field() == (
        core_support_by_field,
        audit_sample_by_field,
        source_witness_by_field,
    )


def test_full_rebuild_scope_does_not_salt_global_block_addresses():
    """Keep global block deduplication independent of logical layout ownership."""

    assert _content_address(7) == _content_address(99)
