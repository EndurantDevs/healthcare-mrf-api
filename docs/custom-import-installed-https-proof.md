# Installed HTTPS proof

This opt-in proof runs the installed CLI outside the checkout, using a disposable
loopback PostgreSQL test database and one reviewed public synthetic Parquet object.
It is separate from ordinary offline tests and does not change CI routing.

Generate the complete fixture with the existing Python environment:

```sh
python -m tests.custom_import_installed_https fixture /approved/output/root_source.parquet
```

The destination must not exist. Review the two synthetic rows and publish the
unchanged bytes at `tests/fixtures/custom_import/https/root_source.parquet` in the
public source repository. Pin its raw HTTPS URL to the full published commit SHA,
not a branch or tag. Keep the printed byte count and SHA-256 in the private proof
packet; a successful generation is not network or runtime evidence.
The local fixture is checked for the exact schema and two synthetic rows, then
its actual byte count and hash bind the HTTPS acquisition and retained capture.
Equivalent Parquet encodings do not require the original encoder version.

After approving the exact URL and a disposable local database, set
`HLTHPRT_CUSTOM_IMPORT_POSTGRES_DSN`, `HLTHPRT_CUSTOM_IMPORT_HTTPS_FIXTURE_FILE`
and `HLTHPRT_CUSTOM_IMPORT_HTTPS_FIXTURE_URL`, then run:

```sh
python -m pytest -q tests/test_custom_import_installed_https_postgres.py \
  --junitxml=/approved/private/packet/installed-https.xml
```

The proof requires Python 3.14 and the existing runtime/test dependencies. It
checks fresh real HTTPS acquisition, exact retained bytes, sealed publication,
paginated/filtered/detail reads, then a separate capture-bound lease-loss case
resumed in a new process with Python socket operations denied except the exact
loopback database. The resolver, TLS, redirect checks and acquisition implementation are
not replaced. Processes retain the existing 30-second command/5-second cleanup
bounds; temporary installed files and owned database schemas use existing cleanup.

This is installed-source evidence, not container-image, throughput or hosted
acceptance. The interruption is controlled lease loss, not an operating-system
crash. Missing fixture configuration skips the opt-in proof; skips are not proof.
