//! Bounded bottom-k selections over closed Source roles or candidate pair rows.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

const SOURCE_CONTRACT: &str = "fhir-source-membership-sample-v1";
const CANDIDATE_CONTRACT: &str = "registry-fhir-sampled-semantic-verification-v1";
const MAX_ROWS: usize = 1_000;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_FIELD_BYTES: usize = 16 * 1024;
const MAX_KEY_BYTES: usize = 128 * 1024;
const MAX_NETWORKS: usize = 4_096;
const MAX_RETAINED_KEYS: usize = 2_000_000;
const MAX_RETAINED_BYTES: usize = 2 * 1024 * 1024 * 1024;
// Covers heap/hash capacity slack, shared-key pointers and allocation metadata.
const SLOT_BYTES: usize = 256;
const ERROR: &str = "network validation sample input or capacity differs";
type Result<T> = std::result::Result<T, &'static str>;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Source,
    Candidate,
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct RankedKey {
    rank: [u8; 32],
    key: Arc<str>,
}

#[derive(Default)]
struct NetworkSelection {
    heap: BinaryHeap<RankedKey>,
    keys: HashSet<Arc<str>>,
    rows: u64,
    roles: [u64; 2],
    overflow: bool,
}

pub struct Coverage {
    pub population: Option<u64>,
    pub population_lower_bound: u64,
    pub provenance_rows: u64,
    pub selected: usize,
    pub role_counts: [u64; 2],
}

#[cfg_attr(feature = "python", pyo3::pyclass(module = "ptg2_address_canon"))]
pub struct NetworkValidationSampler {
    mode: Mode,
    prefix: Vec<u8>,
    source_prefix: Option<String>,
    limit: usize,
    networks: BTreeMap<String, NetworkSelection>,
    retained_bytes: usize,
    roles: [u64; 2],
    failed: bool,
}

fn identity(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-".contains(&byte))
}

fn role_index(kind: &str) -> Result<usize> {
    match kind {
        "OrganizationAffiliation" => Ok(0),
        "PractitionerRole" => Ok(1),
        _ => Err(ERROR),
    }
}

fn checked_increment(value: &mut u64) -> Result<()> {
    *value = value
        .checked_add(1)
        .filter(|count| *count < (1_u64 << 63))
        .ok_or(ERROR)?;
    Ok(())
}

fn candidate_rows_valid(rows: &[[&str; 6]]) -> Result<()> {
    if rows.len() > MAX_ROWS
        || rows
            .iter()
            .flatten()
            .any(|value| value.len() > MAX_FIELD_BYTES)
        || rows
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum::<usize>()
            > MAX_BATCH_BYTES
        || rows.iter().any(|row| !identity(row[0]))
    {
        return Err(ERROR);
    }
    Ok(())
}

fn candidate_key(row: &[&str; 6]) -> Result<String> {
    let key = serde_json::to_string(&[row[0], row[0], row[1], row[2], row[3], row[4], row[5]])
        .map_err(|_| ERROR)?;
    if key.len() > MAX_KEY_BYTES {
        return Err(ERROR);
    }
    Ok(key)
}

impl NetworkValidationSampler {
    pub fn create(
        mode: &str,
        seed: &str,
        networks: &[&str],
        source_base: Option<&str>,
        limit: usize,
    ) -> Result<Self> {
        let mode = match mode {
            "source-role-witness" => Mode::Source,
            "candidate-pair" => Mode::Candidate,
            _ => return Err(ERROR),
        };
        if seed.len() != 64
            || !seed
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !(1..=MAX_ROWS).contains(&limit)
            || networks.is_empty()
            || networks.len() > MAX_NETWORKS
            || networks.len() * limit > MAX_RETAINED_KEYS
            || networks.iter().any(|key| !identity(key))
        {
            return Err(ERROR);
        }
        let source_prefix = match (mode, source_base) {
            (Mode::Source, Some(base)) if !base.is_empty() && base.len() <= MAX_FIELD_BYTES => {
                Some(format!("{}/", base.trim_end_matches('/')))
            }
            (Mode::Candidate, None) => None,
            _ => return Err(ERROR),
        };
        let count = networks.len();
        let networks: BTreeMap<_, _> = networks
            .iter()
            .map(|key| (key.to_string(), NetworkSelection::default()))
            .collect();
        if networks.len() != count {
            return Err(ERROR);
        }
        let contract = if mode == Mode::Source {
            SOURCE_CONTRACT
        } else {
            CANDIDATE_CONTRACT
        };
        Ok(Self {
            mode,
            prefix: format!("{contract}\0{seed}\0").into_bytes(),
            source_prefix,
            limit,
            retained_bytes: count * limit * SLOT_BYTES + count * 512,
            networks,
            roles: [0; 2],
            failed: false,
        })
    }

    fn ready(&self, mode: Mode) -> Result<()> {
        if self.failed || self.mode != mode {
            Err(ERROR)
        } else {
            Ok(())
        }
    }

    fn add_key(&mut self, network: &str, key: String, source_role: Option<usize>) -> Result<()> {
        let Some(selection) = self.networks.get_mut(network) else {
            return Ok(());
        };
        checked_increment(&mut selection.rows)?;
        if let Some(index) = source_role {
            checked_increment(&mut selection.roles[index])?;
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(ERROR);
        }
        if selection.keys.contains(key.as_str()) {
            return Ok(());
        }
        let mut hash = Sha256::new();
        hash.update(&self.prefix);
        if self.mode == Mode::Source {
            hash.update(network.as_bytes());
            hash.update(b"\0");
        }
        hash.update(key.as_bytes());
        let rank = hash.finalize().into();
        if selection.heap.len() == self.limit {
            selection.overflow = true;
            let last = selection.heap.peek().ok_or(ERROR)?;
            if (rank, key.as_str()) >= (last.rank, last.key.as_ref()) {
                return Ok(());
            }
        }
        let removed_bytes = if selection.heap.len() == self.limit {
            selection.heap.peek().ok_or(ERROR)?.key.len()
        } else {
            0
        };
        let retained_bytes = self.retained_bytes - removed_bytes + key.len();
        if retained_bytes > MAX_RETAINED_BYTES {
            return Err(ERROR);
        }
        let key: Arc<str> = key.into();
        if selection.heap.len() == self.limit {
            let removed = selection.heap.pop().ok_or(ERROR)?;
            selection.keys.remove(&removed.key);
        }
        selection.keys.insert(Arc::clone(&key));
        selection.heap.push(RankedKey { rank, key });
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    pub fn push_candidate_batch(&mut self, rows: &[[&str; 6]]) -> Result<()> {
        let result = self.candidate_batch(rows);
        self.failed |= result.is_err();
        result
    }

    fn candidate_batch(&mut self, rows: &[[&str; 6]]) -> Result<()> {
        self.ready(Mode::Candidate)?;
        candidate_rows_valid(rows)?;
        for row in rows {
            if !self.networks.contains_key(row[0]) {
                continue;
            }
            self.add_key(row[0], candidate_key(row)?, None)?;
        }
        Ok(())
    }

    pub fn candidate_match_indices(&self, rows: &[[&str; 6]]) -> Result<Vec<usize>> {
        self.ready(Mode::Candidate)?;
        candidate_rows_valid(rows)?;
        let mut selected = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            if let Some(network) = self.networks.get(row[0]) {
                if network.keys.contains(candidate_key(row)?.as_str()) {
                    selected.push(index);
                }
            }
        }
        Ok(selected)
    }

    pub fn push_source_batch(&mut self, rows: &[(&str, &str, &str)]) -> Result<()> {
        let result = self.source_batch(rows);
        self.failed |= result.is_err();
        result
    }

    fn source_batch(&mut self, rows: &[(&str, &str, &str)]) -> Result<()> {
        self.ready(Mode::Source)?;
        if rows.len() > MAX_ROWS
            || rows
                .iter()
                .map(|(kind, id, refs)| kind.len() + id.len() + refs.len())
                .sum::<usize>()
                > MAX_BATCH_BYTES
        {
            return Err(ERROR);
        }
        for (kind, id, references) in rows {
            let index = role_index(kind)?;
            if !identity(id) {
                return Err(ERROR);
            }
            let references: serde_json::Value =
                serde_json::from_str(references).map_err(|_| ERROR)?;
            let mut targets = HashSet::new();
            if !references.is_null() {
                for reference in references.as_array().ok_or(ERROR)? {
                    let reference = reference.as_str().ok_or(ERROR)?;
                    let prefix = self.source_prefix.as_deref().ok_or(ERROR)?;
                    let relative = reference.strip_prefix(prefix).unwrap_or(reference);
                    if let Some(network) = relative.strip_prefix("Organization/") {
                        if identity(network) && self.networks.contains_key(network) {
                            targets.insert(network.to_owned());
                        }
                    }
                }
            }
            checked_increment(&mut self.roles[index])?;
            for network in targets {
                self.add_key(&network, format!("{kind}\0{id}"), Some(index))?;
            }
        }
        Ok(())
    }

    pub fn selected_rows(&self, network: &str) -> Result<Vec<Vec<String>>> {
        if self.failed {
            return Err(ERROR);
        }
        let selection = self.networks.get(network).ok_or(ERROR)?;
        let mut selected: Vec<_> = selection.heap.iter().collect();
        selected.sort_unstable();
        selected
            .into_iter()
            .map(|entry| {
                if self.mode == Mode::Source {
                    Ok(entry.key.split('\0').map(str::to_owned).collect())
                } else {
                    let mut row: Vec<String> =
                        serde_json::from_str(&entry.key).map_err(|_| ERROR)?;
                    row.remove(0);
                    Ok(row)
                }
            })
            .collect()
    }

    pub fn network_coverage(&self, network: &str) -> Result<Coverage> {
        if self.failed {
            return Err(ERROR);
        }
        let selection = self.networks.get(network).ok_or(ERROR)?;
        let selected = selection.keys.len();
        let population = if self.mode == Mode::Source {
            Some(selection.rows)
        } else if selection.overflow {
            None
        } else {
            Some(selected as u64)
        };
        Ok(Coverage {
            population,
            population_lower_bound: population.unwrap_or(self.limit as u64 + 1),
            provenance_rows: selection.rows,
            selected,
            role_counts: selection.roles,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sampler(mode: &str, limit: usize) -> NetworkValidationSampler {
        NetworkValidationSampler::create(
            mode,
            &"0".repeat(64),
            &["a", "empty"],
            (mode == "source-role-witness").then_some("https://example.org/fhir"),
            limit,
        )
        .unwrap()
    }

    #[test]
    fn candidate_sample_matches_python_canonical_unicode_rank_and_duplicates() {
        let rows = [
            ["a", "http://example.org/id", "123", "l1", "a1", "café"],
            ["a", "id", "456", "l2", "a2", "a\"b\\c\n"],
            ["a", "id", "789", "l3", "a3", "x\0y"],
            ["a", "id", "000", "l4", "a4", "雪"],
            ["a", "id", "001", "l5", "a5", ""],
        ];
        let mut forward = sampler("candidate-pair", 2);
        forward.push_candidate_batch(&rows).unwrap();
        forward.push_candidate_batch(&rows).unwrap();
        let expected: Vec<Vec<String>> = [rows[3], rows[1]]
            .iter()
            .map(|row| row.iter().map(|value| value.to_string()).collect())
            .collect();
        assert_eq!(forward.selected_rows("a").unwrap(), expected);
        let coverage = forward.network_coverage("a").unwrap();
        assert_eq!(
            (
                coverage.population,
                coverage.population_lower_bound,
                coverage.provenance_rows,
                coverage.selected
            ),
            (None, 3, 10, 2)
        );
        let mut backward = sampler("candidate-pair", 2);
        for row in rows.iter().rev() {
            backward.push_candidate_batch(&[*row]).unwrap();
        }
        assert_eq!(backward.selected_rows("a").unwrap(), expected);
        assert_eq!(
            forward.network_coverage("empty").unwrap().population,
            Some(0)
        );
    }

    #[test]
    fn selected_duplicates_preserve_exact_small_population() {
        let mut sample = sampler("candidate-pair", 2);
        let row = ["a", "id", "1", "l", "a", "c"];
        sample.push_candidate_batch(&[row, row]).unwrap();
        let coverage = sample.network_coverage("a").unwrap();
        assert_eq!(
            (
                coverage.population,
                coverage.provenance_rows,
                coverage.selected
            ),
            (Some(1), 2, 1)
        );
        assert_eq!(sample.selected_rows("unknown").err(), Some(ERROR));
    }

    #[test]
    fn candidate_batch_filter_preserves_all_selected_witnesses_and_input_indices() {
        let mut sample = sampler("candidate-pair", 1);
        let first = ["a", "id", "1", "l", "a", "c"];
        let outside = ["other", "id", "1", "l", "a", "c"];
        sample.push_candidate_batch(&[first, outside]).unwrap();
        assert_eq!(
            sample
                .candidate_match_indices(&[outside, first, outside, first])
                .unwrap(),
            vec![1, 3]
        );
        assert_eq!(sample.network_coverage("a").unwrap().provenance_rows, 1);
        assert!(sample
            .candidate_match_indices(&vec![first; MAX_ROWS + 1])
            .is_err());
        assert!(sampler("source-role-witness", 1)
            .candidate_match_indices(&[first])
            .is_err());
    }

    #[test]
    fn source_selection_matches_python_and_counts_refs_once_per_role() {
        let rows = [
            ("PractitionerRole", "p1", "[\"Organization/a\",\"https://example.org/fhir/Organization/a\",\"https://foreign.example/Organization/a\"]"),
            ("OrganizationAffiliation", "o1", "[\"Organization/a\"]"),
            ("PractitionerRole", "p2", "[\"Organization/a\"]"),
            ("OrganizationAffiliation", "o2", "[\"Organization/a\"]"),
            ("PractitionerRole", "p3", "null"),
        ];
        let mut forward = sampler("source-role-witness", 2);
        forward.push_source_batch(&rows).unwrap();
        assert_eq!(
            forward.selected_rows("a").unwrap(),
            vec![
                vec!["PractitionerRole", "p2"],
                vec!["PractitionerRole", "p1"]
            ]
        );
        let coverage = forward.network_coverage("a").unwrap();
        assert_eq!(
            (coverage.population, coverage.selected, coverage.role_counts),
            (Some(4), 2, [2, 2])
        );
        assert_eq!(forward.roles, [2, 3]);
        let mut reverse = sampler("source-role-witness", 2);
        for row in rows.iter().rev() {
            reverse.push_source_batch(&[*row]).unwrap();
        }
        assert_eq!(
            forward.selected_rows("a").unwrap(),
            reverse.selected_rows("a").unwrap()
        );
    }

    #[test]
    fn source_reference_refusals_poison_partial_selection() {
        for refs in ["{}", "42", "[null]", "[1]", "["] {
            let mut sample = sampler("source-role-witness", 2);
            assert!(
                sample
                    .push_source_batch(&[
                        ("PractitionerRole", "p", "[\"Organization/a\"]"),
                        ("PractitionerRole", "q", refs)
                    ])
                    .is_err(),
                "{refs}"
            );
            assert!(sample.selected_rows("a").is_err());
            assert!(sample.network_coverage("a").is_err());
            assert!(sample.push_source_batch(&[]).is_err());
        }
        for (kind, id) in [
            ("Organization", "p"),
            ("PractitionerRole", "bad/id"),
            ("PractitionerRole", ""),
        ] {
            assert!(sampler("source-role-witness", 2)
                .push_source_batch(&[(kind, id, "null")])
                .is_err());
        }
    }

    #[test]
    fn external_references_never_strip_a_foreign_host() {
        let mut sample = sampler("source-role-witness", 2);
        sample.push_source_batch(&[("PractitionerRole", "p", "[\"\",\"Practitioner/a\",\"Organization/a?x=1\",\"Organization/a/_history/1\",\"https://example.org/fhir/Practitioner/a\",\"https://[broken/Organization/a\",\"https://[::1%]/Organization/a\",\"https://foreign.example：443/Organization/a\",\"https://foreign.example/Organization/a\",\"https://example.org/fhir-other/Organization/a\",\"https://[::1]/Organization/a\",\"https://foreign.example/a?x=1#b\"]")]).unwrap();
        let long = serde_json::to_string(&["x".repeat(2049)]).unwrap();
        sample
            .push_source_batch(&[("PractitionerRole", "q", &long)])
            .unwrap();
        assert!(sample.selected_rows("a").unwrap().is_empty());
        assert_eq!(sample.roles, [0, 2]);
    }

    #[test]
    fn constructor_batch_field_memory_and_mode_bounds_fail_closed() {
        for (mode, seed, keys, base, limit) in [
            ("other", "0".repeat(64), vec!["a"], None, 1),
            ("candidate-pair", "A".repeat(64), vec!["a"], None, 1),
            ("candidate-pair", "0".repeat(64), vec!["a", "a"], None, 1),
            ("candidate-pair", "0".repeat(64), vec!["bad/key"], None, 1),
            ("candidate-pair", "0".repeat(64), vec!["a"], None, 0),
            ("candidate-pair", "0".repeat(64), vec!["a"], None, 1001),
            ("source-role-witness", "0".repeat(64), vec!["a"], None, 1),
            (
                "candidate-pair",
                "0".repeat(64),
                vec!["a"],
                Some("https://example.org"),
                1,
            ),
        ] {
            assert!(NetworkValidationSampler::create(mode, &seed, &keys, base, limit).is_err());
        }
        let row = ["a", "id", "1", "l", "a", "c"];
        assert!(sampler("candidate-pair", 2)
            .push_candidate_batch(&vec![row; MAX_ROWS + 1])
            .is_err());
        let long = "x".repeat(MAX_FIELD_BYTES + 1);
        assert!(sampler("candidate-pair", 2)
            .push_candidate_batch(&[["a", &long, "1", "l", "a", "c"]])
            .is_err());
        let large = "x".repeat(MAX_FIELD_BYTES);
        assert!(sampler("candidate-pair", 2)
            .push_candidate_batch(&vec![["a", &large, &large, &large, &large, &large]; 200])
            .is_err());
        let mut sample = sampler("candidate-pair", 2);
        sample.retained_bytes = MAX_RETAINED_BYTES;
        assert!(sample.push_candidate_batch(&[row]).is_err());
        assert!(sample.selected_rows("a").is_err());
        assert!(sampler("candidate-pair", 2).push_source_batch(&[]).is_err());
        assert!(sampler("source-role-witness", 2)
            .push_candidate_batch(&[])
            .is_err());
    }

    #[test]
    fn skewed_stream_retains_a_bounded_distinct_sample() {
        let mut sample = sampler("candidate-pair", 1_000);
        for start in (0..5_000).step_by(1_000) {
            let ids: Vec<_> = (start..start + 1_000)
                .map(|index| index.to_string())
                .collect();
            let rows: Vec<_> = ids
                .iter()
                .map(|id| ["a", "id", id.as_str(), "l", "a", "c"])
                .collect();
            sample.push_candidate_batch(&rows).unwrap();
            sample.push_candidate_batch(&rows).unwrap();
        }
        let coverage = sample.network_coverage("a").unwrap();
        assert_eq!(
            (
                coverage.population,
                coverage.population_lower_bound,
                coverage.provenance_rows,
                coverage.selected
            ),
            (None, 1001, 10_000, 1_000)
        );
        assert_eq!(
            sample.networks["a"].heap.len(),
            sample.networks["a"].keys.len()
        );
        assert!(sample.retained_bytes < 1024 * 1024);
    }
}

#[cfg(feature = "python")]
mod python {
    use super::*;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::pybacked::PyBackedStr;
    use pyo3::types::{PyDict, PyTuple};

    fn refusal(error: &'static str) -> PyErr {
        PyValueError::new_err(error)
    }

    fn bounded_rows(rows: &Bound<'_, PyTuple>) -> PyResult<()> {
        if !rows.is_exact_instance_of::<PyTuple>() || rows.len() > MAX_ROWS {
            return Err(refusal(ERROR));
        }
        Ok(())
    }

    #[pymethods]
    impl NetworkValidationSampler {
        #[new]
        #[pyo3(signature = (mode, seed, network_keys, source_base=None, limit=1000))]
        fn new(
            mode: &str,
            seed: &str,
            network_keys: &Bound<'_, PyTuple>,
            source_base: Option<&str>,
            limit: usize,
        ) -> PyResult<Self> {
            if network_keys.len() > MAX_NETWORKS {
                return Err(refusal(ERROR));
            }
            let keys: Vec<PyBackedStr> = network_keys.extract()?;
            let keys: Vec<_> = keys.iter().map(|key| &**key).collect();
            Self::create(mode, seed, &keys, source_base, limit).map_err(refusal)
        }

        fn add_candidate_batch(
            &mut self,
            py: Python<'_>,
            rows: &Bound<'_, PyTuple>,
        ) -> PyResult<()> {
            bounded_rows(rows)?;
            let rows: Vec<[PyBackedStr; 6]> = rows.extract()?;
            py.detach(|| {
                let rows: Vec<_> = rows
                    .iter()
                    .map(|row| std::array::from_fn(|index| &*row[index]))
                    .collect();
                self.push_candidate_batch(&rows)
            })
            .map_err(refusal)
        }

        fn add_source_batch(&mut self, py: Python<'_>, rows: &Bound<'_, PyTuple>) -> PyResult<()> {
            bounded_rows(rows)?;
            let rows: Vec<(PyBackedStr, PyBackedStr, PyBackedStr)> = rows.extract()?;
            py.detach(|| {
                let rows: Vec<_> = rows
                    .iter()
                    .map(|(kind, id, refs)| (&**kind, &**id, &**refs))
                    .collect();
                self.push_source_batch(&rows)
            })
            .map_err(refusal)
        }

        fn candidate_selected_indices(
            &self,
            py: Python<'_>,
            rows: &Bound<'_, PyTuple>,
        ) -> PyResult<Vec<usize>> {
            bounded_rows(rows)?;
            let rows: Vec<[PyBackedStr; 6]> = rows.extract()?;
            py.detach(|| {
                let rows: Vec<_> = rows
                    .iter()
                    .map(|row| std::array::from_fn(|index| &*row[index]))
                    .collect();
                self.candidate_match_indices(&rows)
            })
            .map_err(refusal)
        }

        fn selected(&self, py: Python<'_>, network: &str) -> PyResult<Vec<Vec<String>>> {
            py.detach(|| self.selected_rows(network)).map_err(refusal)
        }

        fn network_keys(&self) -> PyResult<Vec<String>> {
            if self.failed {
                return Err(refusal(ERROR));
            }
            Ok(self.networks.keys().cloned().collect())
        }

        fn coverage<'py>(&self, py: Python<'py>, network: &str) -> PyResult<Bound<'py, PyDict>> {
            let coverage = self.network_coverage(network).map_err(refusal)?;
            let result = PyDict::new(py);
            result.set_item("population", coverage.population)?;
            result.set_item("population_lower_bound", coverage.population_lower_bound)?;
            result.set_item("provenance_rows", coverage.provenance_rows)?;
            result.set_item("selected", coverage.selected)?;
            result.set_item(
                "series_counts",
                (0..10)
                    .map(|index| coverage.selected.saturating_sub(index * 100).min(100))
                    .collect::<Vec<_>>(),
            )?;
            let roles = PyDict::new(py);
            roles.set_item("OrganizationAffiliation", coverage.role_counts[0])?;
            roles.set_item("PractitionerRole", coverage.role_counts[1])?;
            result.set_item("role_counts", roles)?;
            Ok(result)
        }

        fn role_counts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
            self.ready(Mode::Source).map_err(refusal)?;
            let result = PyDict::new(py);
            result.set_item("OrganizationAffiliation", self.roles[0])?;
            result.set_item("PractitionerRole", self.roles[1])?;
            Ok(result)
        }

        fn retained_bytes_upper_bound(&self) -> PyResult<usize> {
            if self.failed {
                return Err(refusal(ERROR));
            }
            Ok(self.retained_bytes)
        }
    }
}
