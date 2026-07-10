# PTG2 Optimization Research Harness

This workflow keeps PTG2 optimization work on `research/ptg2-optimization-harness`
until a measured subset is explicitly promoted to `main`.

## Branch Rules

- Create the branch from `origin/main`.
- Keep generated benchmark evidence under `reports/ptg2-experiments/`; that path
  is ignored by Git through `reports/*`.
- Do not change default deployed PTG2 scanner behavior from this branch unless a
  local fixture gate and a dev pilot gate pass.
- Keep the PTG2 DB publish dedupe fallback enabled.

## Local Usage

List the configured cases:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py list
```

Run a dry-run plan:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py run --dry-run
```

Run fixture benchmarks after building the scanner:

```bash
cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case large-in-network-fixture \
  --case duplicate-serving-fixture
```

Compare default worker-side raw chunks with a memory-sensitive 1 MiB raw chunk
cap on a bulky synthetic file:

```bash
cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case bulky-raw-chunk-fixture \
  --variant parse_in_workers \
  --variant raw_chunk_1m
```

Inspect `elapsed_seconds`, `peak_rss_kb`, `raw_chunk_count`,
`raw_chunk_max_bytes`, and matching COPY output digests. This case intentionally
skips the performance gate because smaller chunks can trade some throughput for
lower peak memory and more predictable huge-file scheduling.

Run the local DB-backed PTG smoke against PostgreSQL on `127.0.0.1:5440`.
This serves a tiny TOC and in-network file from a temporary localhost HTTP
server, enables `HLTHPRT_FETCH_ALLOW_LOCAL=true` only for the child process, and
targets `healthporta_test` via `HLTHPRT_DB_DATABASE=healthporta` plus
`HLTHPRT_TEST_DATABASE_SUFFIX=_test`:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case local-db-smoke
```

Run a full-file local import and verify the published DB snapshot against the
original generated source file. Unlike `local-db-smoke`, this does not pass
`--max-items`; after `PTG2_IMPORT_DONE`, the harness reloads `rates.json.gz`,
computes independent source counts and a sorted price-atom digest, then compares
those values with the published serving, price atom, and provider-member tables:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case local-full-file-verify
```

### Normalized Membership Graph A/B

Run the PostgreSQL-only v1/v2 membership comparison with:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/ptg2_membership_graph_local_suite.json
```

The 2026-07-10 64K-rate control run validated every source price and NPI in both
architectures with API caches disabled:

| Metric | `postgres_binary_v1` | `postgres_binary_v2` |
| --- | ---: | ---: |
| Snapshot bytes | 396,480 | 372,984 |
| Import seconds | 0.80 | 0.66 |
| Code lookup p95 | 14.08 ms | 12.66 ms |
| NPI reverse p95 | 17.38 ms | 14.69 ms |
| ZIP + 100-price-set response p95 | 51.17 ms | 55.22 ms |

The small fixture has one NPI per provider set, so v2 is only about 6% smaller
there. It intentionally does not model the expensive payer shape that motivated
v2: many provider sets containing large shared groups. In that shape, v1 stores
the provider-group membership table plus a direct set-to-NPI expansion; v2
stores each group/NPI edge once in each direction and keeps only a distinct-NPI
scope table. Use a real high-fan-out source comparison before estimating fleet
storage savings from the control fixture.

The suite also includes a TIN-only v2 case. It verifies that a valid rate file
with zero NPIs still publishes all unique prices, creates an empty indexed NPI
scope, returns no reverse-NPI items, and leaves no scope stage tables behind.

### Real-scale dev findings

A 2026-07-10 dev run measured a 13.80 GiB compressed dental source with
513,262,462 unique serving rows. The source and plan names are intentionally
omitted here; the persisted snapshot/report ids remain the operational source
of truth.

| Stage | Actual | Measured floor or current dependency floor | Interpretation |
| --- | ---: | ---: | --- |
| Raw file read, warm page cache | 6.19 s | 6.19 s | 2.23 GiB/s; storage was not the scanner constraint |
| Clean single-core `gzip -dc` | 541.51 s | 541.51 s | 26.10 compressed MiB/s; hard floor for the current single gzip member |
| Rust scanner | 876.70 s | 541.51 s | 1.62x the gzip floor; at most 335.19 s is removable without changing the container format |
| Complete data phase | 931.58 s | 541.51 s | Includes about 54.88 s after scanner completion |
| Parallel staging COPY | 85.76 s | Not isolated in this run | 650,988,716 rows, or 7.59M rows/s |
| By-code binary stream | 452.89 s | Runs concurrently with reverse | 513.26M rows and 1.679 GB output |
| Reverse binary stream | 566.51 s | 566.51 s pair wall floor | 513.26M rows and 2.656 GB output |
| Price-set/atom stream | 746.28 s | Not isolated in this run | 29.12M price sets, 127.33M atom refs, and 2.521 GB payload |
| Binary build | 1,312.92 s | 1,312.79 s with current dependencies | `max(452.89, 566.51) + 746.28`; wrapper overhead is only 0.13 s |
| Remaining serving publish | 73.79 s | Stage-specific | Mostly the 66.27 s lean price-atom rewrite; artifact upload was 2.96 s |
| Full PostgreSQL publish | 2,404.13 s | See scenario bounds below | 40m04.13s; this measured snapshot predated the logged-relation durability fix |

The scanner captured 272.64 GiB of raw negotiated-rate JSON and sustained
318.44 MiB/s across its workers. The producer was blocked on worker queues for
175.32 s, exactly 20.0% of scanner time and 52.3% of the scanner headroom above
clean gzip. Removing all observed queue blocking would still leave the scanner
159.87 s above the gzip floor, so worker throughput and producer scanning both
matter.

The measured snapshot tables were later found to be `UNLOGGED`. PostgreSQL can
truncate unlogged relations after an unclean restart, so the 40-minute run was
PostgreSQL-resident but not a valid durability benchmark. New binary snapshots
create retained relations as logged tables and verify every table in the
published `materialized_tables` map before cutover. The first post-deploy import
must therefore remeasure publish time and WAL cost rather than treating the old
40-minute number as the logged baseline.

The stage arithmetic gives useful scenario bounds:

- Bringing only the scanner to the measured gzip floor changes 40m04s to about
  34m29s. Scanner tuning cannot produce the largest remaining gain.
- The new writer runs the price-set/atom work concurrently with the two serving
  streams and moves its row encoding into bounded-memory Rust. Dependency
  arithmetic lowers the uncontended binary-build bound from 21m53s to at most
  12m26s, changing the old full-run arithmetic to about 30m38s. This remains a
  projection until a logged production-scale publish measures PostgreSQL sort,
  COPY, WAL, and table-extension contention.
- Combining both optimistic changes gives about 25m02s. This is a scheduling
  bound, not a benchmark promise; three ordered PostgreSQL streams may contend.
- The bounded Rust atom encoder removes Python row iteration but cannot remove
  PostgreSQL's ordered scan or sort.

The writer persists per-kind staging input bytes and elapsed throughput, plus
PostgreSQL source bytes, target bytes, time-to-first-byte, and completion time
for each Rust stream. These counters let the next comparison separate database
sort/export, Rust encoding, and target COPY instead of treating each stream as
one opaque timer. It also records the final relation persistence so an unlogged
snapshot cannot be mistaken for a durable result again.

The real storage A/B also constrains the architecture claim:

| Shape | Compatibility layout | Normalized v2 | Change |
| --- | ---: | ---: | ---: |
| 513M serving rows | 8.017 GiB | 7.783 GiB | 239.6 MiB smaller, 2.9% |
| 520M serving rows | 7.989 GiB | 7.809 GiB | 183.8 MiB smaller, 2.2% |

For both shapes, the 7.2 GiB serving binary and roughly 0.9 GiB price-atom
table dominate; v2 changes provider membership, not those components. A
different dense source exposed the opposite extreme: its two provider-set/group
directions alone occupied 3.766 GiB stored because they represented about
1.12B edges. Replacing one direction with a request-time inversion would save
space but make one API direction scan the complete edge set. Any next storage
version therefore needs compact bidirectional integer-key blocks and a shared
price dictionary, not ordinary one-edge-per-row PostgreSQL tables.

### Candidate 2-3 GB architecture

The measured cardinalities support a separate, immutable
`postgres_binary_v3` experiment rather than another in-place v2 rewrite. For
the 513M-row source, only 952,060 code/provider groups have more than one price
set. A v3 layout can therefore keep one sharded forward projection, replace the
full reverse projection with provider/code bitmaps plus a sparse multiplicity
stream, store the 29.12M price-set ids once, and encode 127.33M atom references
with 24-bit dense atom keys. The key width must be snapshot-adaptive: use 24
bits only below 16,777,216 unique atoms, switch automatically to 32 bits above
that boundary, and record the selected width in every block header and manifest.
Binary atom payloads can then replace the UUID-heavy relational atom heap and
index without imposing a 24-bit limit on larger imports.

The current measured estimate is 2.63-2.99 GB decimal, or 2.45-2.78 GiB:

| Candidate v3 component | Estimated bytes |
| --- | ---: |
| Sharded forward projection | 1.15-1.35 GB |
| Shared price-set dictionary | 0.48-0.50 GB |
| Reverse provider/code bitmap and overflow | 0.17-0.18 GB |
| Dense price-set/atom membership | 0.40-0.43 GB |
| Binary atom payloads | 0.14-0.18 GB |
| Provider graph, NPI scope, support, and allowance | 0.29-0.36 GB |

The v3 reader and Rust streaming writer now implement this shape. Price-set
keys remain 32-bit, atom widths are selected from complete snapshot
cardinality, provider/code pairs use bounded spill runs plus a streaming k-way
merge, and missing referenced blocks fail closed. This remains a real-source
design budget until a dense production pilot proves its final PostgreSQL footprint;
synthetic scaling is evidence for the encoding, not proof of the 2-3 GiB
headline.

### Current v3 evidence (2026-07-10)

A cache-disabled local PostgreSQL run with 4,194,304 serving rates, 4,096 code
keys, and 1,024 provider sets produced a complete 14,742,560-byte snapshot. The
same fixture's previous v2 result was 35,935,248 bytes, so v3 was 2.44 times
smaller. The v3 run completed in 9.8 seconds, including a 3.977-second binary
build. With 20 measured requests after two warmups, code lookup p95 was
34.435 ms and NPI reverse p95 was 39.850 ms; both passed the explicit 40 ms
p95 gate with binary, dictionary, and sidecar caches disabled. The report is:

```text
/tmp/ptg2-v3-final-4m-report/run-20260710T194138Z/report.json
```

A separate fresh-process run with zero warmups measured the first code request
at 59.704 ms and the following NPI reverse request at 86.436 ms. The current
validated contract is therefore warm p95 at or below 40 ms plus a 100 ms cold
sample ceiling; do not describe v3 as a universal 40 ms path until snapshot
prewarming or fewer PostgreSQL round trips closes that first-request gap. The
cold report is:

```text
/tmp/ptg2-v3-cold-4m-report/run-20260710T195429Z/report.json
```

One production-scale sample was roughly 14.8 GB compressed and 293 GB expanded.
On the dev PTGHuge lane, verified decode-to-stdout took 124 seconds with four
rapidgzip threads and 111 seconds with eight, versus the 523-second GNU gzip
floor. Four threads are the initial integrated-scanner choice because parsing
workers need the remaining CPU; the full scanner and publish phases still need
the real import measurement.

A synthetic reversed-order fixture with 960,000 negotiated rates and about
179 MB of expanded JSON took 48.845 seconds through the checked serial fallback
and 7.332 seconds through the indexed 16-worker path, a 6.66x wall-time
improvement. Both produced the same 168 unique COPY rows and SHA-256 after
deduplication. The temporary `gztool` index was 102,915 bytes; the default
`indexed_gzip` format was about 46x larger on the same input.

Before promotion, deploy readers first, preserve side-by-side immutable
snapshots, compare logical tuple checksums against v2, verify forward, reverse,
geo, and price hydration parity, and complete the real-source storage and
memory pilot.

After taxonomy filtering was moved ahead of the v2 location candidate limit,
the warmed PostgreSQL-only API p95 values were 20.48 ms for code-to-provider,
8.94 ms for one NPI/code, 36.94 ms for all prices of one NPI, and 22.66 ms for
radius geo. Reverse result digests matched v1 exactly. Geo-forward returns more
providers because v1 applies another limit after provider-group fan-out, which
can discard already selected NPIs; do not force v2 to reproduce that
under-coverage.

Reports are written to:

```text
reports/ptg2-experiments/run-<timestamp>/report.json
reports/ptg2-experiments/run-<timestamp>/report.md
```

## Dev Pilot

The pilot case is skipped unless a control URL and token are supplied. Use the
engine control API URL in dev, or the equivalent import-control proxy if that is
the selected entry point:

```bash
HLTHPRT_CONTROL_URL=http://127.0.0.1:8080/control/v1 \
HLTHPRT_CONTROL_API_TOKEN=<token> \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case ptg-pilot-import \
  --variant parse_in_workers
```

The pilot payload uses existing PTG lane fields:

- `_expected_queue`
- `_expected_worker_class`
- `_scanner_rust_workers`
- `_scanner_parse_in_workers`
- `_scanner_work_queue`
- `_scanner_event_queue`

## Gates

- Correctness: sorted COPY row counts and SHA-256 digests must match baseline.
- Dedupe: negotiated-rate and serving-rate dedupe counters must match expected
  baseline behavior.
- Performance: candidate elapsed time must improve by at least 15 percent.
- Memory: candidate peak RSS must not grow more than 20 percent unless the
  report assigns it to a larger PTG resource lane.

Unknown memory is non-fatal for local macOS runs because `/proc` is unavailable;
dev-server runs should use pod/cgroup evidence before promotion.

## Dev Deploy Shape

When gates pass, build a research image:

```bash
TAG=ghcr.io/endurantdevs/healthcare-mrf-api:research-ptg2-$(git rev-parse --short HEAD)-$(date -u +%Y%m%d%H%M%S)
```

Load or publish that tag for `ns1033171`, update the dev deploy branch image
pins, reconcile Flux, and run the pilot import again. Roll back by restoring the
previous image pin in `healthporta-deploy`.
