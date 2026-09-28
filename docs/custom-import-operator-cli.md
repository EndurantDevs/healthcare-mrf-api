# Custom-import operator CLI

Run these commands from an installed application environment with the existing
database configuration. They use the standalone engine; no external control
service is required. IDs below are synthetic examples, not default targets.

Validate a bounded definition from standard input without opening the database:

```sh
python -m custom_import_cli validate --format json < definition.json
```

Inspect an exact execution or retained generation:

```sh
python -m custom_import_cli status --dataset-id 5 --execution-id 17
python -m custom_import_cli status --dataset-id 5 --generation-id 19
```

Request cooperative cancellation; a terminal execution can return an unchanged
receipt. This does not guarantee that an active worker has already stopped:

```sh
python -m custom_import_cli cancel --execution-id 17
```

Activate an initial generation only while the current pointer is absent:

```sh
python -m custom_import_cli activate --dataset-id 5 \
  --target-generation-id 19 --expected-pointer-version 0
```

For an existing pointer, supply both the observed generation and pointer version.
Rollback selects an exact retained generation, using the same compare-and-swap
precondition:

```sh
python -m custom_import_cli activate --dataset-id 5 \
  --target-generation-id 20 --expected-generation-id 19 --expected-pointer-version 1
python -m custom_import_cli rollback --dataset-id 5 \
  --target-generation-id 19 --expected-generation-id 20 --expected-pointer-version 2
```

Publication still applies the engine's existing seal, ownership, and retention
checks. A stale pointer fails with a redacted conflict receipt; inspect current
state before choosing new preconditions. Commands never automatically retry.

Success emits a compact JSON receipt after transaction commit and connection
cleanup. Errors omit database diagnostics and record payloads. A connection
cleanup failure can occur after commit: a failed command does not prove rollback.
Reconcile exact state with `status` before another mutation.

Use `--help` on the entry point or any command for its arguments. Generic resume,
discovery, preview, and estimate are not exposed by this entry point yet.
