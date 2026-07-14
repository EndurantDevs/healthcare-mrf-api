# Strict V3 Finalizer Resource Contract

Every strict `postgres_binary_v3` finalizer invocation requires an explicit
resource profile. There are no hidden Rust defaults in the production wrapper.

| Environment variable | Rust argument | Process-wide meaning |
| --- | --- | --- |
| `HLTHPRT_PTG2_V3_FINALIZER_WORKERS` | `--workers` | Concurrent finalizer workers, from 1 through 64. |
| `HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES` | `--identity-map-max-bytes` | Combined cap for all dense identity maps. The value must be MiB-aligned and between 64 MiB and 512 GiB. |
| `HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES` | `--total-sort-memory-bytes` | One aggregate sort-memory budget shared by every worker. It must divide into MiB-aligned worker shares, provide at least 16 MiB per worker, and be no greater than 256 GiB. |

The wrapper rejects `HLTHPRT_PTG2_V3_FINALIZER_MEMORY_RECORDS`. A record limit
can accidentally become a per-worker multiplier as concurrency changes. When a
finite cgroup or address-space limit is visible, identity-map and sort budgets
together may consume at most three quarters of it and must leave at least 1 GiB
for dictionaries, projections, encoders, queues, and the runtime.

The selected values and wrapper validation are written into the finalizer input
manifest. Rust must treat `--total-sort-memory-bytes` as one process-wide budget,
divide it among active sort workers, and echo the exact resource contract in the
final summary. A missing or different echo fails publication. The validated
contract is retained in finalizer progress/report metadata so an import timing
can be interpreted against the resources that produced it.

## Rust CLI Requirement

The Rust finalizer must implement `--total-sort-memory-bytes` as an aggregate
cap across all active sort workers. It must not reinterpret the value as a
per-worker allowance or convert it into a per-worker record limit. The final
summary must echo:

- contract `ptg2_v3_finalizer_resources_v1`;
- worker count;
- combined identity-map cap;
- total sort-memory budget;
- sort scope `process_total_across_workers_v1`.

The Python wrapper validates that exact echo before returning a finalizer
summary. This makes an older or incompatible Rust binary fail closed before
publication.
