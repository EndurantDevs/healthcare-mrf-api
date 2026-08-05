use ptg2_scanner::tax_identity::{TaxIdentityState, TaxIdentityStateV2};
use ptg2_scanner::tax_identity_sidecar_bundle::{
    audit_tax_identity_sidecar_bundle, audit_tax_identity_sidecar_bundle_with_progress,
    finalize_tax_identity_sidecar_bundle, validate_tax_identity_sidecar_shard,
    ProviderGroupUniverse, TaxIdentityCollisionAuditConfig, TaxIdentityCollisionAuditLimits,
    TaxIdentityCollisionAuditPhase, TaxIdentitySidecarBundleCheckpoint,
    TaxIdentitySidecarV1Admission, TaxIdentitySidecarV2ArtifactDescriptor,
    TaxIdentitySidecarV2Metadata, TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT,
    TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT,
    TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES, TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT,
    TAX_IDENTITY_COLLISION_CHECK_PASSED, TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY,
    TAX_IDENTITY_LOCATOR_COLLISION_POLICY, TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT,
    TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY,
    TAX_IDENTITY_SIDECAR_FULL_HMAC_AUTHORITY_CONTRACT, TAX_IDENTITY_SIDECAR_HMAC_CONTRACT,
    TAX_IDENTITY_SIDECAR_LOCATOR_CONTRACT, TAX_IDENTITY_SIDECAR_TOKEN_MESSAGE_CONTRACT,
    TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT, TAX_IDENTITY_SIDECAR_V2_NORMALIZATION_CONTRACT,
};
use ptg2_scanner::tax_identity_sidecar_v1::{
    TaxIdentitySidecarV1Header, TaxIdentitySidecarV1Record, TAX_IDENTITY_SIDECAR_V1_FORMAT,
    TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION, TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES,
};
use ptg2_scanner::tax_identity_sidecar_v2::{
    TaxIdentitySidecarV2Header, TaxIdentitySidecarV2Record, TAX_IDENTITY_SIDECAR_V2_FORMAT,
    TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION, TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const POLICY: &str = "ptg-tin-hmac-sha256-v1:collision-audit-integration";
const V1_RESOURCE_NAME: &str = "provider_group_tax_identity";
const V2_RESOURCE_NAME: &str = "provider_group_tax_identity_v2";
type RowSpec = (TaxIdentityStateV2, Option<[u8; 32]>);
type V1Row = ([u8; 16], TaxIdentityState, Option<[u8; 32]>);
type V2Row = ([u8; 16], TaxIdentityStateV2, Option<[u8; 32]>);

struct BundleFixture {
    temporary: TempDir,
    checkpoint: TaxIdentitySidecarBundleCheckpoint,
    descriptors: Vec<TaxIdentitySidecarV2ArtifactDescriptor>,
}

impl BundleFixture {
    fn new(shards: Vec<Vec<RowSpec>>) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let mut checkpoints = Vec::new();
        let mut descriptors = Vec::new();
        for (shard_index, rows) in shards.into_iter().enumerate() {
            let shard_id = format!("synthetic-{shard_index:04}");
            let groups = (0..rows.len())
                .map(|row_index| group(shard_index, row_index))
                .collect::<Vec<_>>();
            let v1_rows = rows
                .iter()
                .zip(&groups)
                .map(|((state, hmac), group)| v1_row(*group, *state, *hmac))
                .collect::<Vec<_>>();
            let v2_rows = rows
                .iter()
                .zip(&groups)
                .map(|((state, hmac), group)| (*group, *state, *hmac))
                .collect::<Vec<_>>();
            let v1_bytes = encode_v1(&v1_rows);
            let v2_bytes = encode_v2(&v2_rows);
            let v1_path = temporary.path().join(format!("{shard_id}.v1"));
            let v2_path = temporary.path().join(format!("{shard_id}.v2"));
            fs::write(&v1_path, &v1_bytes).unwrap();
            fs::write(&v2_path, &v2_bytes).unwrap();
            let descriptor = descriptor(&shard_id, &rows, &v2_path, &v2_bytes);
            let counts = v1_counts(&v1_rows);
            let v1_sha256 = hex(&Sha256::digest(&v1_bytes));
            let admission = TaxIdentitySidecarV1Admission {
                path: &v1_path,
                record_format: TAX_IDENTITY_SIDECAR_V1_FORMAT,
                sha256: &v1_sha256,
                byte_count: v1_bytes.len() as u64,
                row_count: rows.len() as u64,
                provider_group_count: rows.len() as u64,
                matched_ein_count: counts[0],
                missing_count: counts[1],
                malformed_count: counts[2],
                unsupported_type_count: counts[3],
                version: TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION,
                record_bytes: TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES as u16,
                token_policy_id: POLICY,
                normalization_contract: TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT,
                hmac_contract: TAX_IDENTITY_SIDECAR_HMAC_CONTRACT,
                final_file: true,
                name: Some(V1_RESOURCE_NAME),
                source_shard_id: Some(&shard_id),
                shard_id: None,
            };
            checkpoints.push(
                validate_tax_identity_sidecar_shard(
                    &shard_id,
                    admission,
                    &descriptor,
                    &VecUniverse(groups),
                )
                .unwrap(),
            );
            descriptors.push(descriptor);
        }
        Self {
            temporary,
            checkpoint: finalize_tax_identity_sidecar_bundle(checkpoints).unwrap(),
            descriptors,
        }
    }

    fn scratch_root(&self, suffix: &str) -> PathBuf {
        let root = self.temporary.path().join(format!("scratch-{suffix}"));
        fs::create_dir(&root).unwrap();
        root
    }
}

#[test]
fn public_api_runs_bounded_merge_and_exposes_redacted_contracts() {
    let fixture = BundleFixture::new(vec![
        vec![
            matched(TaxIdentityStateV2::MatchedEin, 1),
            matched(TaxIdentityStateV2::MatchedNpi, 2),
            matched(TaxIdentityStateV2::MatchedEin, 3),
            matched(TaxIdentityStateV2::MatchedNpi, 4),
            matched(TaxIdentityStateV2::MatchedEin, 5),
            matched(TaxIdentityStateV2::MatchedNpi, 6),
        ],
        vec![
            matched(TaxIdentityStateV2::MatchedEin, 7),
            matched(TaxIdentityStateV2::MatchedNpi, 8),
            matched(TaxIdentityStateV2::MatchedEin, 9),
            matched(TaxIdentityStateV2::MatchedNpi, 10),
            matched(TaxIdentityStateV2::MatchedEin, 1),
            matched(TaxIdentityStateV2::MatchedNpi, 6),
        ],
    ]);
    let scratch = fixture.scratch_root("forced");
    let forced_config = config(scratch.clone(), 361_505, 2, 6);
    let mut descriptors = fixture.descriptors.clone();
    descriptors.reverse();
    let mut events = Vec::new();
    let result = audit_tax_identity_sidecar_bundle_with_progress(
        &fixture.checkpoint,
        &descriptors,
        &forced_config,
        |event| {
            events.push(event);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(forced_config.scratch_root(), scratch);
    assert_eq!(forced_config.limits().max_artifacts, 64);
    assert!(format!("{forced_config:?}").contains("<redacted>"));
    assert_eq!(
        events.last().unwrap().phase,
        TaxIdentityCollisionAuditPhase::Complete
    );
    assert!(events
        .windows(2)
        .all(|pair| { pair[0].phase != pair[1].phase || pair[0].completed <= pair[1].completed }));

    let checkpoint = result.checkpoint();
    let observed_contracts = [
        checkpoint.contract(),
        checkpoint.record_contract(),
        checkpoint.occurrence_digest_contract(),
        checkpoint.locator_collision_policy(),
        checkpoint.full_hmac_type_collision_policy(),
        checkpoint.same_identity_repetition_policy(),
        checkpoint.multi_candidate_locator_support(),
        checkpoint.locator_prefix_collision_check(),
        checkpoint.full_hmac_cross_type_collision_check(),
    ];
    assert_eq!(
        observed_contracts,
        [
            TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT,
            TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT,
            TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT,
            TAX_IDENTITY_LOCATOR_COLLISION_POLICY,
            TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY,
            TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY,
            TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT,
            TAX_IDENTITY_COLLISION_CHECK_PASSED,
            TAX_IDENTITY_COLLISION_CHECK_PASSED,
        ]
    );
    assert!(!checkpoint.publication_admissible());
    assert_eq!(checkpoint.projection_authority(), "v1_only");
    assert_eq!(
        checkpoint.source_bundle_sha256(),
        fixture.checkpoint.bundle_sha256()
    );
    assert_eq!(checkpoint.matched_row_count(), 12);
    assert_eq!(checkpoint.unique_identity_count(), 10);
    assert_eq!(
        (
            checkpoint.matched_ein_count(),
            checkpoint.matched_npi_count()
        ),
        (6, 6)
    );
    assert_eq!(
        (
            checkpoint.repeated_identity_count(),
            checkpoint.repeated_occurrence_count()
        ),
        (2, 2)
    );
    assert_eq!(checkpoint.occurrence_multiset_sha256().len(), 64);
    assert_eq!(checkpoint.audit_sha256().len(), 64);

    let stats = result.stats();
    assert_eq!((stats.source_rows, stats.matched_rows), (12, 12));
    assert_eq!(
        (stats.initial_run_count, stats.merge_operation_count),
        (12, 10)
    );
    assert_eq!(stats.maximum_merge_fan_in, 2);
    assert_eq!(stats.cancellation_poll_count, events.len() as u64);
    let debug = format!("{result:?}");
    assert!(debug.contains("TaxIdentityCollisionAuditResult"));
    assert!(!debug.contains(checkpoint.audit_sha256()));
    assert!(directory_is_empty(&scratch));

    let roomy_scratch = fixture.scratch_root("roomy");
    let roomy = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(roomy_scratch.clone(), 1_000_000, 3, 7),
    )
    .unwrap();
    assert_eq!(roomy.checkpoint(), checkpoint);
    assert_eq!(roomy.stats().initial_run_count, 1);
    assert!(directory_is_empty(&roomy_scratch));
}

#[test]
fn public_api_handles_empty_matches_collisions_and_resource_limits() {
    let empty = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::Missing, None),
        (TaxIdentityStateV2::Malformed, None),
        (TaxIdentityStateV2::UnsupportedType, None),
    ]]);
    let absent_scratch = empty.temporary.path().join("unused-scratch");
    let empty_result = audit_tax_identity_sidecar_bundle(
        &empty.checkpoint,
        &empty.descriptors,
        &config(absent_scratch.clone(), 1_000_000, 2, 6),
    )
    .unwrap();
    assert_eq!(empty_result.checkpoint().matched_row_count(), 0);
    assert_eq!(empty_result.stats().initial_run_count, 0);
    assert!(!absent_scratch.exists());

    let shared = token(0x41);
    let cross_type = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(shared)),
        (TaxIdentityStateV2::MatchedNpi, Some(shared)),
    ]]);
    assert_eq!(
        audit_fixture(&cross_type, "cross-type")
            .unwrap_err()
            .to_string(),
        "PTG tax identity full HMAC cross-type collision detected"
    );

    let mut same_locator = shared;
    same_locator[31] = 0x42;
    let locator = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(shared)),
        (TaxIdentityStateV2::MatchedEin, Some(same_locator)),
    ]]);
    assert_eq!(
        audit_fixture(&locator, "locator").unwrap_err().to_string(),
        "PTG tax identity locator prefix collision detected"
    );

    let valid = BundleFixture::new(vec![vec![matched(TaxIdentityStateV2::MatchedEin, 7)]]);
    let scratch = valid.scratch_root("limits");
    let mut limits = config(scratch.clone(), 1_000_000, 2, 6).limits();
    limits.max_artifacts = 0;
    let mut events = Vec::new();
    let error = audit_tax_identity_sidecar_bundle_with_progress(
        &valid.checkpoint,
        &valid.descriptors,
        &TaxIdentityCollisionAuditConfig::new(scratch.clone(), limits),
        |event| {
            events.push(event);
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "PTG tax identity collision audit artifact limit exceeded"
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        (events[0].phase, events[0].completed, events[0].total),
        (TaxIdentityCollisionAuditPhase::Admission, 0, 1)
    );

    limits.max_artifacts = 64;
    limits.max_source_rows = 0;
    let error = audit_tax_identity_sidecar_bundle(
        &valid.checkpoint,
        &valid.descriptors,
        &TaxIdentityCollisionAuditConfig::new(scratch.clone(), limits),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "PTG tax identity collision audit row limit exceeded"
    );
    assert!(directory_is_empty(&scratch));
}

fn descriptor(
    shard_id: &str,
    rows: &[RowSpec],
    path: &Path,
    bytes: &[u8],
) -> TaxIdentitySidecarV2ArtifactDescriptor {
    let counts = state_counts(rows);
    TaxIdentitySidecarV2ArtifactDescriptor {
        path: path.to_owned(),
        metadata: TaxIdentitySidecarV2Metadata {
            record_format: TAX_IDENTITY_SIDECAR_V2_FORMAT.to_owned(),
            sha256: hex(&Sha256::digest(bytes)),
            byte_count: bytes.len() as u64,
            row_count: rows.len() as u64,
            provider_group_count: rows.len() as u64,
            matched_ein_count: counts[0],
            matched_npi_count: counts[1],
            missing_count: counts[2],
            malformed_count: counts[3],
            unsupported_type_count: counts[4],
            version: TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION,
            record_bytes: TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES as u16,
            token_policy_id: POLICY.to_owned(),
            normalization_contract: TAX_IDENTITY_SIDECAR_V2_NORMALIZATION_CONTRACT.to_owned(),
            token_message_contract: TAX_IDENTITY_SIDECAR_TOKEN_MESSAGE_CONTRACT.to_owned(),
            hmac_contract: TAX_IDENTITY_SIDECAR_HMAC_CONTRACT.to_owned(),
            tin_id_128_contract: TAX_IDENTITY_SIDECAR_LOCATOR_CONTRACT.to_owned(),
            full_hmac_authority_contract: TAX_IDENTITY_SIDECAR_FULL_HMAC_AUTHORITY_CONTRACT
                .to_owned(),
            final_file: true,
            name: V2_RESOURCE_NAME.to_owned(),
            source_shard_id: shard_id.to_owned(),
        },
    }
}

struct VecUniverse(Vec<[u8; 16]>);

impl ProviderGroupUniverse for VecUniverse {
    fn provider_group_count(&self) -> u64 {
        self.0.len() as u64
    }

    fn provider_group_at(&self, index: u64) -> io::Result<[u8; 16]> {
        self.0.get(index as usize).copied().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "synthetic universe mismatch")
        })
    }
}

fn v1_row(group: [u8; 16], state: TaxIdentityStateV2, hmac: Option<[u8; 32]>) -> V1Row {
    match state {
        TaxIdentityStateV2::MatchedEin => (group, TaxIdentityState::MatchedEin, hmac),
        TaxIdentityStateV2::MatchedNpi | TaxIdentityStateV2::UnsupportedType => {
            (group, TaxIdentityState::UnsupportedType, None)
        }
        TaxIdentityStateV2::Missing => (group, TaxIdentityState::Missing, None),
        TaxIdentityStateV2::Malformed => (group, TaxIdentityState::Malformed, None),
    }
}

fn state_counts(rows: &[RowSpec]) -> [u64; 5] {
    let mut counts = [0; 5];
    for (state, _) in rows {
        counts[match state {
            TaxIdentityStateV2::MatchedEin => 0,
            TaxIdentityStateV2::MatchedNpi => 1,
            TaxIdentityStateV2::Missing => 2,
            TaxIdentityStateV2::Malformed => 3,
            TaxIdentityStateV2::UnsupportedType => 4,
        }] += 1;
    }
    counts
}

fn v1_counts(rows: &[V1Row]) -> [u64; 4] {
    let mut counts = [0; 4];
    for (_, state, _) in rows {
        counts[match state {
            TaxIdentityState::MatchedEin => 0,
            TaxIdentityState::Missing => 1,
            TaxIdentityState::Malformed => 2,
            TaxIdentityState::UnsupportedType => 3,
        }] += 1;
    }
    counts
}

fn encode_v1(rows: &[V1Row]) -> Vec<u8> {
    let mut bytes = TaxIdentitySidecarV1Header::new(POLICY.to_owned())
        .unwrap()
        .encode();
    for (group, state, hmac) in rows {
        let hmac = hmac.unwrap_or([0; 32]);
        let record =
            TaxIdentitySidecarV1Record::new(*group, *state, hmac[..16].try_into().unwrap(), hmac)
                .unwrap();
        bytes.extend_from_slice(&record.encode());
    }
    bytes
}

fn encode_v2(rows: &[V2Row]) -> Vec<u8> {
    let mut bytes = TaxIdentitySidecarV2Header::new(POLICY.to_owned())
        .unwrap()
        .encode();
    for (group, state, hmac) in rows {
        let hmac = hmac.unwrap_or([0; 32]);
        let record =
            TaxIdentitySidecarV2Record::new(*group, *state, hmac[..16].try_into().unwrap(), hmac)
                .unwrap();
        bytes.extend_from_slice(&record.encode());
    }
    bytes
}

fn group(shard_index: usize, row_index: usize) -> [u8; 16] {
    let mut group = [0; 16];
    group[..8].copy_from_slice(&(shard_index as u64 + 1).to_be_bytes());
    group[8..].copy_from_slice(&(row_index as u64 + 1).to_be_bytes());
    group
}

fn matched(state: TaxIdentityStateV2, value: u8) -> RowSpec {
    (state, Some(token(value)))
}

fn token(value: u8) -> [u8; 32] {
    [value; 32]
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn config(
    scratch_root: PathBuf,
    max_memory_bytes: u64,
    merge_fan_in: usize,
    max_open_files: usize,
) -> TaxIdentityCollisionAuditConfig {
    TaxIdentityCollisionAuditConfig::new(
        scratch_root,
        TaxIdentityCollisionAuditLimits {
            max_artifacts: 64,
            max_source_rows: 10_000,
            max_matched_rows: 10_000,
            max_memory_bytes,
            max_scratch_bytes: 10_000 * TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64 * 2,
            minimum_free_scratch_bytes: 0,
            merge_fan_in,
            max_open_files,
        },
    )
}

fn audit_fixture(fixture: &BundleFixture, suffix: &str) -> io::Result<()> {
    let scratch = fixture.scratch_root(suffix);
    let result = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
    )
    .map(|_| ());
    assert!(directory_is_empty(&scratch));
    result
}

fn directory_is_empty(path: &Path) -> bool {
    fs::read_dir(path).unwrap().next().is_none()
}
