//! Cardinality-bounded global dictionaries for the provider graph model.

use super::{
    advance_build_progress, invalid, invalid_conversion, GlobalId, ProgressReporter,
    ProviderGraphV4Options, ProviderGraphV4Result, ResourceAdmissionTracker,
    V4ProviderGraphShardDescriptor, V4ResourceAdmissionSummary, DENSE_FORMAT,
    REFERENCE_SPOOL_FIXED_BYTES, TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES,
    TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES, TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND,
    TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES,
};
use std::collections::HashSet;
use std::{fs, path::Path};

// Each raw occurrence is a 16-byte global ID in a geometrically grown Vec.
// Each dense incidence has two u32 directions, each with capacity <= 2*len.
// The owner allowance covers raw hash buckets, unique/global dictionaries,
// dense lookup buckets, vector headers and allocation rounding. Reciprocal
// input occurrences and tax rows remain charged even when not materialized.
// Member-only IDs need dictionaries and vector headers even without a matching
// owner (for example a group with no NPI). Charge these separately using dense
// dictionary cardinality, or occurrence count for legacy non-dense artifacts.
// Derived patterns, tax projections and scratch retain their separate gates.
pub(super) fn factor_storage_upper_bound_bytes(
    edges: u64,
    owners: u64,
    member_globals: u64,
) -> ProviderGraphV4Result<u64> {
    edges
        .checked_mul(48)
        .and_then(|bytes| {
            owners
                .checked_mul(384)
                .and_then(|extra| bytes.checked_add(extra))
        })
        .and_then(|bytes| {
            member_globals
                .checked_mul(256)
                .and_then(|extra| bytes.checked_add(extra))
        })
        .ok_or_else(|| invalid("resource_admission: factor storage byte count overflows"))
}

pub(super) fn unique_sorted_globals(
    globals: impl Iterator<Item = GlobalId>,
    progress: &mut ProgressReporter<'_>,
    completed: &mut u64,
    total: u64,
) -> ProviderGraphV4Result<Vec<GlobalId>> {
    let mut unique = HashSet::new();
    for global in globals {
        unique.insert(global);
        advance_build_progress(progress, completed, total)?;
    }
    let mut globals: Vec<_> = unique.into_iter().collect();
    globals.sort_unstable();
    globals.shrink_to_fit();
    Ok(globals)
}

pub(super) fn resource_admission_preflight(
    descriptors: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: &Path,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<ResourceAdmissionTracker> {
    if descriptors.is_empty() {
        return Err(invalid("V4 provider graph requires at least one shard"));
    }
    let mut input_factor_bytes = 0u64;
    let mut factor_edge_count = 0u64;
    let mut factor_owner_count = 0u64;
    let mut member_global_upper_bound = 0u64;
    let mut matched_ein_occurrence_upper_bound = 0u64;
    let shard_count = invalid_conversion(
        u64::try_from(descriptors.len()),
        "resource_admission: shard count exceeds uint64",
    )?;
    let source_bitmap_bytes = shard_count.checked_add(7).ok_or(invalid(
        "resource_admission: tax identity bitmap width overflows",
    ))? / 8;
    let mut tax_identity_group_occurrence_upper_bound = 0u64;
    // This covers the ordinal vector, cloned shard IDs, and the temporary
    // uniqueness set before any factor mmap is opened.
    let mut tax_identity_source_ordinal_upper_bound_bytes = shard_count
        .checked_mul(TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES)
        .ok_or(invalid(
            "resource_admission: tax identity source ordinal bytes overflow",
        ))?;
    for shard in descriptors {
        for artifact in [
            &shard.provider_set_component,
            &shard.provider_component_group,
            &shard.provider_group_npi,
            &shard.provider_npi_group,
        ] {
            input_factor_bytes = input_factor_bytes
                .checked_add(artifact.metadata.byte_count)
                .ok_or(invalid("resource_admission: input byte count overflows"))?;
            factor_edge_count = factor_edge_count
                .checked_add(artifact.metadata.member_count)
                .ok_or(invalid("resource_admission: factor edge count overflows"))?;
            let member_globals = if artifact.metadata.record_format == DENSE_FORMAT {
                artifact
                    .metadata
                    .member_global_count
                    .unwrap_or(artifact.metadata.member_count)
            } else {
                artifact.metadata.member_count
            };
            member_global_upper_bound = member_global_upper_bound
                .checked_add(member_globals)
                .ok_or(invalid("resource_admission: member global count overflows"))?;
            factor_owner_count = factor_owner_count
                .checked_add(artifact.metadata.owner_count)
                .ok_or(invalid("resource_admission: factor owner count overflows"))?;
        }
        input_factor_bytes = input_factor_bytes
            .checked_add(shard.provider_group_tax_identity.metadata.byte_count)
            .ok_or(invalid("resource_admission: input byte count overflows"))?;
        factor_edge_count = factor_edge_count
            .checked_add(shard.provider_group_tax_identity.metadata.row_count)
            .ok_or(invalid("resource_admission: factor edge count overflows"))?;
        factor_owner_count = factor_owner_count
            .checked_add(
                shard
                    .provider_group_tax_identity
                    .metadata
                    .provider_group_count,
            )
            .ok_or(invalid("resource_admission: factor owner count overflows"))?;
        tax_identity_group_occurrence_upper_bound = tax_identity_group_occurrence_upper_bound
            .checked_add(
                shard
                    .provider_group_tax_identity
                    .metadata
                    .provider_group_count,
            )
            .ok_or(invalid(
                "resource_admission: tax identity group occurrence count overflows",
            ))?;
        matched_ein_occurrence_upper_bound = matched_ein_occurrence_upper_bound
            .checked_add(shard.provider_group_tax_identity.metadata.matched_ein_count)
            .ok_or(invalid(
                "resource_admission: matched tax identity count overflows",
            ))?;
        let shard_id_bytes = invalid_conversion(
            u64::try_from(shard.shard_id.len()),
            "resource_admission: shard ID length exceeds uint64",
        )?;
        tax_identity_source_ordinal_upper_bound_bytes =
            tax_identity_source_ordinal_upper_bound_bytes
                .checked_add(
                    shard_id_bytes
                        .checked_mul(TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND)
                        .ok_or(invalid(
                            "resource_admission: tax identity source ID bytes overflow",
                        ))?,
                )
                .ok_or(invalid(
                    "resource_admission: tax identity source ordinal bytes overflow",
                ))?;
    }
    let tax_identity_merge_bitmap_upper_bound_bytes = tax_identity_group_occurrence_upper_bound
        .checked_mul(source_bitmap_bytes)
        .ok_or(invalid(
            "resource_admission: tax identity merge bitmap bytes overflow",
        ))?;
    let tax_identity_projection_upper_bound_bytes = tax_identity_group_occurrence_upper_bound
        .checked_mul(TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES.saturating_add(source_bitmap_bytes))
        .and_then(|value| {
            value.checked_add(
                matched_ein_occurrence_upper_bound
                    .saturating_mul(TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES),
            )
        })
        .ok_or(invalid(
            "resource_admission: tax identity projection upper bound overflows",
        ))?;
    let provider_set_key_map_bytes = match fs::metadata(provider_set_key_map_path) {
        Ok(metadata) => metadata.len(),
        Err(error) => {
            return Err(invalid(format!(
                "resource_admission: provider-set key map is unavailable: {error}"
            )));
        }
    };
    // Raw 16-byte IDs reserve at most twice the declared occurrences. Dense
    // forward/reverse u32 relations reserve twice their lengths. Charge both
    // phases together even though raw owners are consumed during conversion;
    // repeated global dictionaries are cardinality-bounded, not edge-sized.
    let factor_storage_bytes = factor_storage_upper_bound_bytes(
        factor_edge_count,
        factor_owner_count,
        member_global_upper_bound,
    )?;
    let base_estimated_model_bytes = input_factor_bytes
        .checked_add(provider_set_key_map_bytes.saturating_mul(4))
        .and_then(|value| value.checked_add(factor_storage_bytes))
        .and_then(|value| value.checked_add(tax_identity_merge_bitmap_upper_bound_bytes))
        .and_then(|value| value.checked_add(tax_identity_source_ordinal_upper_bound_bytes))
        .ok_or(invalid(
            "resource_admission: estimated peak byte count overflows",
        ))?;
    // Emission holds one relation-member page, one locator page, and at most
    // one streamed heavy-bitmap page at the same time. Reference rows are
    // externally spooled by object kind, so their resident memory is bounded
    // by a fixed number of small writer buffers rather than block count.
    let bounded_emission_buffer_bytes = invalid_conversion(
        u64::try_from(options.member_page_bytes),
        "resource_admission: member page bytes exceed uint64",
    )?
    .checked_mul(2)
    .and_then(|value| value.checked_add(options.locator_page_bytes as u64))
    .and_then(|value| value.checked_add(REFERENCE_SPOOL_FIXED_BYTES))
    .ok_or(invalid(
        "resource_admission: emission buffer byte count overflows",
    ))?;
    let estimated_peak_bytes = base_estimated_model_bytes
        .checked_add(tax_identity_projection_upper_bound_bytes)
        .and_then(|value| value.checked_add(bounded_emission_buffer_bytes))
        .ok_or(invalid(
            "resource_admission: estimated peak byte count overflows",
        ))?;
    if options
        .max_factor_edges
        .is_some_and(|limit| factor_edge_count > limit)
    {
        return Err(invalid(format!(
            "resource_admission: factor edge count {factor_edge_count} exceeds configured limit {}",
            options.max_factor_edges.expect("checked above")
        )));
    }
    if options
        .max_estimated_model_bytes
        .is_some_and(|limit| estimated_peak_bytes > limit)
    {
        return Err(invalid(format!(
            "resource_admission: estimated peak bytes {estimated_peak_bytes} exceeds configured limit {}",
            options.max_estimated_model_bytes.expect("checked above")
        )));
    }
    Ok(ResourceAdmissionTracker {
        summary: V4ResourceAdmissionSummary {
            formula: "base(input_factor_bytes + provider_set_key_map_bytes*4 + factor_edges*(32_raw_capacity + 16_dense_reciprocal_capacity) + factor_owners*384 + member_global_upper_bound*256 + tax_identity_merge_bitmap_upper_bound_bytes + tax_identity_source_ordinal_upper_bound_bytes) + derived_projection_bytes + tax_identity_projection_bytes(preflight=tax_identity_projection_upper_bound_bytes,reconciled=exact) + retained_scratch_high_water_bytes + bounded_emission_buffer_bytes"
                .to_string(),
            input_factor_bytes,
            provider_set_key_map_bytes,
            factor_edge_count,
            factor_owner_count,
            tax_identity_merge_bitmap_upper_bound_bytes,
            tax_identity_source_ordinal_upper_bound_bytes,
            tax_identity_projection_upper_bound_bytes,
            base_estimated_model_bytes,
            derived_projection_bytes: 0,
            tax_identity_projection_bytes: tax_identity_projection_upper_bound_bytes,
            retained_scratch_high_water_bytes: 0,
            bounded_emission_buffer_bytes,
            estimated_peak_bytes,
            max_estimated_model_bytes: options.max_estimated_model_bytes,
            max_factor_edges: options.max_factor_edges,
        },
        tax_identity_projection_reconciled: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_graph_v4::V4ProgressEvent;

    #[test]
    fn storage_ledger_charges_both_phases_and_rejects_overflow() {
        assert_eq!(
            factor_storage_upper_bound_bytes(100, 10, 20).unwrap(),
            13760
        );
        assert!(factor_storage_upper_bound_bytes(u64::MAX, 0, 0).is_err());
        assert!(factor_storage_upper_bound_bytes(0, u64::MAX, 0).is_err());
        assert!(factor_storage_upper_bound_bytes(0, 0, u64::MAX).is_err());
    }

    #[test]
    fn repeated_occurrences_do_not_retain_occurrence_sized_storage() {
        let mut sink = |_event: &V4ProgressEvent| {};
        let mut progress = ProgressReporter::new(&mut sink);
        let mut completed = 0;
        let input = (0..100_000).map(|index| [((index % 3) + 1) as u8; 16]);
        let globals = unique_sorted_globals(input, &mut progress, &mut completed, 100_000).unwrap();
        assert_eq!(globals, vec![[1; 16], [2; 16], [3; 16]]);
        assert!(globals.capacity() <= 2 * globals.len());
        assert_eq!(completed, 100_000);
    }
}
