# 04 MRF Chunk Concurrency

## Goal

MRF imports should use file-level and chunk-level concurrency without turning
one giant parser into the only active CPU consumer.

## Required Behavior

- Keep `HLTHPRT_MRF_FILE_CHUNKING=providers,formularies` as the dev default.
- Keep chunking disabled for small files below `HLTHPRT_MRF_CHUNK_MIN_MB`.
- Use larger COPY/write batches:
  - plans: `HLTHPRT_MRF_PLAN_FLUSH_ROWS=2000`
  - providers: `HLTHPRT_MRF_PROVIDER_FLUSH_ROWS=50000`
  - formularies: `HLTHPRT_MRF_FORMULARY_FLUSH_ROWS=50000`
- Track chunk count, queued/running/completed chunks, data seconds, finish
  seconds, and rows loaded in run metrics.

## Future Split

The next split can add dedicated MRF chunk lanes after the resource metrics show
which file families actually need separate memory profiles.
