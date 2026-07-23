use ptg2_scanner::v3_runs::{
    external_sort_assigned_serving_records, external_sort_dedupe_partition_files,
    external_sort_dense_ids, external_sort_lexicographic_records, external_sort_partition_files,
    external_sort_provider_code_pairs, external_sort_provider_identities,
    external_sort_tagged_partition_files, merge_sorted_provider_identities,
    read_audit_candidate_file, write_audit_candidate_file, AssignedServingRunBuilder,
    AssignedServingRunMerger, AuditCandidateRecord, ScratchDurability, ServingRunRecord,
    TaggedServingRunCodec, ASSIGNED_SERVING_RECORD_BYTES,
};
use std::fs;
use std::path::Path;

fn write_records(path: &Path, records: &[Vec<u8>]) {
    let bytes = records.iter().flatten().copied().collect::<Vec<_>>();
    fs::write(path, bytes).unwrap();
}

fn serving_record(code: u8, provider: u8, price: u8) -> ServingRunRecord {
    ServingRunRecord {
        code_id: [code; 16],
        provider_set_id: [provider; 16],
        price_set_id: [price; 16],
        provider_count: 7,
    }
}

#[test]
fn importer_fixed_width_sort_surfaces_cover_every_record_family() {
    let root = tempfile::tempdir().unwrap();
    let dense_input = root.path().join("dense-input");
    write_records(&dense_input, &[vec![2; 16], vec![1; 16], vec![1; 16]]);
    let dense = external_sort_dense_ids(
        &[dense_input],
        root.path().join("dense-output"),
        root.path().join("dense-scratch"),
        1,
    )
    .unwrap();
    assert_eq!((dense.unique_records, dense.duplicate_records), (2, 1));

    let identity_input = root.path().join("identity-input");
    let mut identity_one = vec![1; 16];
    identity_one.extend_from_slice(&1u32.to_be_bytes());
    let mut identity_two = vec![2; 16];
    identity_two.extend_from_slice(&2u32.to_be_bytes());
    write_records(
        &identity_input,
        &[identity_two, identity_one.clone(), identity_one],
    );
    let identity_output = root.path().join("identity-output");
    let identities = external_sort_provider_identities(
        &[identity_input],
        &identity_output,
        root.path().join("identity-scratch"),
        1,
    )
    .unwrap();
    assert_eq!(
        (identities.unique_records, identities.duplicate_records),
        (2, 1)
    );
    let merged = merge_sorted_provider_identities(
        &[identity_output.clone(), identity_output],
        root.path().join("identity-merged"),
    )
    .unwrap();
    assert_eq!((merged.unique_records, merged.duplicate_records), (2, 2));

    let pair_input = root.path().join("pair-input");
    write_records(&pair_input, &[vec![2; 8], vec![1; 8], vec![1; 8]]);
    let pairs = external_sort_provider_code_pairs(
        &[pair_input],
        root.path().join("pair-output"),
        root.path().join("pair-scratch"),
        1,
    )
    .unwrap();
    assert_eq!((pairs.unique_records, pairs.duplicate_records), (2, 1));

    let assigned_input = root.path().join("assigned-input");
    write_records(
        &assigned_input,
        &[
            vec![2; ASSIGNED_SERVING_RECORD_BYTES],
            vec![1; ASSIGNED_SERVING_RECORD_BYTES],
            vec![1; ASSIGNED_SERVING_RECORD_BYTES],
        ],
    );
    let assigned = external_sort_assigned_serving_records(
        &[assigned_input],
        root.path().join("assigned-output"),
        root.path().join("assigned-scratch"),
        1,
    )
    .unwrap();
    assert_eq!(
        (assigned.unique_records, assigned.duplicate_records),
        (3, 0)
    );

    let lexical_input = root.path().join("lexical-input");
    write_records(&lexical_input, &[vec![2; 4], vec![1; 4], vec![1; 4]]);
    let lexical = external_sort_lexicographic_records(
        &[lexical_input],
        root.path().join("lexical-output"),
        root.path().join("lexical-scratch"),
        4,
        1,
        true,
    )
    .unwrap();
    assert_eq!((lexical.unique_records, lexical.duplicate_records), (2, 1));
}

#[test]
fn importer_serving_sort_audit_and_bounded_builder_surfaces_are_exact() {
    let root = tempfile::tempdir().unwrap();
    let serving_input = root.path().join("serving-input");
    let records = [
        serving_record(2, 2, 2),
        serving_record(1, 1, 1),
        serving_record(1, 1, 1),
    ];
    let mut serving_bytes = Vec::new();
    for record in &records {
        record.write_to(&mut serving_bytes).unwrap();
    }
    fs::write(&serving_input, serving_bytes).unwrap();

    let sorted = external_sort_partition_files(
        std::slice::from_ref(&serving_input),
        root.path().join("serving-output"),
        root.path().join("serving-scratch"),
        1,
        0,
        1,
    )
    .unwrap();
    assert_eq!(sorted.input_records, 3);
    assert_eq!(sorted.unique_records, 2);
    let deduped = external_sort_dedupe_partition_files(
        std::slice::from_ref(&serving_input),
        root.path().join("serving-deduped"),
        root.path().join("serving-dedupe-scratch"),
        1,
        0,
        1,
    )
    .unwrap();
    assert_eq!((deduped.unique_records, deduped.duplicate_records), (2, 1));

    let codec = TaggedServingRunCodec::new(2, 1).unwrap();
    assert_eq!((codec.source_count(), codec.source_key_bytes()), (2, 1));
    let tagged = external_sort_tagged_partition_files(
        &[(serving_input, 1)],
        root.path().join("tagged-output"),
        root.path().join("tagged-scratch"),
        1,
        0,
        1,
        codec,
    )
    .unwrap();
    assert_eq!(tagged.input_records, 3);

    let audit = AuditCandidateRecord {
        code_key: 1,
        provider_set_key: 2,
        price_key: 3,
        source_key: 4,
        provider_count: 5,
    };
    assert!(AuditCandidateRecord::decode(&[]).is_err());
    let audit_path = root.path().join("audit");
    write_audit_candidate_file(&audit_path, 1, &[audit]).unwrap();
    assert_eq!(read_audit_candidate_file(&audit_path).unwrap(), vec![audit]);

    let builder_root = root.path().join("assigned-builder");
    let mut builder = AssignedServingRunBuilder::with_scratch_durability(
        &builder_root,
        1,
        ScratchDurability::Ephemeral,
    )
    .unwrap();
    builder.push([2; ASSIGNED_SERVING_RECORD_BYTES]).unwrap();
    builder.push([1; ASSIGNED_SERVING_RECORD_BYTES]).unwrap();
    let run_set = builder.finish().unwrap();
    assert_eq!(run_set.stats.input_records, 2);
    let mut merger = AssignedServingRunMerger::new(&run_set.paths).unwrap();
    assert_eq!(
        merger.next_record().unwrap(),
        Some([1; ASSIGNED_SERVING_RECORD_BYTES])
    );
    assert_eq!(
        merger.next_record().unwrap(),
        Some([2; ASSIGNED_SERVING_RECORD_BYTES])
    );
    assert_eq!(merger.next_record().unwrap(), None);

    for call in [
        external_sort_dense_ids(&[], root.path().join("bad-dense"), root.path(), 0),
        external_sort_provider_code_pairs(&[], root.path().join("bad-pair"), root.path(), 0),
        external_sort_assigned_serving_records(
            &[],
            root.path().join("bad-assigned"),
            root.path(),
            0,
        ),
    ] {
        assert!(call.is_err());
    }
    assert!(AssignedServingRunBuilder::new(root.path().join("bad-builder"), 0).is_err());
}
