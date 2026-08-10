# Official-file FHIR formulary operator

The official-file adapter projects the public CMS drug JSON linked by the
retained IFP and Community & State catalog observations into the existing
immutable `formulary_fhir` repository. It does not invoke the legacy formulary
importer and does not write legacy plan-drug tables.

The catalog contract requires exactly 24 IFP and 24 Community & State drug
files. Listing bytes are acquired once by the provider-file catalog workflow;
this adapter derives its drug-only identity from those retained bytes. Each
drug file is installed once in the shared content-addressed artifact store.
Retries and both independent normalization roots reopen those exact bytes
instead of downloading or storing another copy.

## Acquire and admit

Acquisition is a default-off one-shot operation. The selector is the exact
retained two-listing observation hash; selecting an implicit latest observation
is not supported.

```console
HLTHPRT_UHC_FORMULARY_ACQUISITION_ENABLED=true \
HLTHPRT_UHC_FORMULARY_PUBLICATION_ENABLED=false \
HLTHPRT_UHC_FORMULARY_WORK_DIRECTORY=/work/uhc-formulary \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/uhc_formulary_operator.py \
  acquire-admit --raw-set-sha256 64_LOWERCASE_HEX_CHARACTERS
```

The work directory must already exist, resolve without symlinks, belong to the
runtime UID, and grant no group or other permissions. Acquisition fails closed
unless all 48 advertised files download as nonempty top-level JSON object
arrays and match the retained identity contract. A source URL that returns an
error document is not skipped and no partial catalog can be admitted.

After artifact verification, the operator derives one canonical cutoff from
the latest immutable artifact verification timestamp. It independently builds
two private SQLite spools, writes two full repository roots, admits their exact
PostgreSQL graph match, and records a durable UHC admission receipt. Admission
does not advance the current formulary pointer.

The JSON result contains only bounded counts, opaque run/dataset/receipt IDs,
timestamps, and content hashes. It contains no drug, plan, NPI, or URL values.
An interrupted run is retried with the same selector; verified artifacts and
completed immutable checkpoints are reused.

## Publish one receipt

Publication is a separate default-off operation and accepts only the durable
receipt ID returned by acquisition.

```console
HLTHPRT_UHC_FORMULARY_ACQUISITION_ENABLED=false \
HLTHPRT_UHC_FORMULARY_PUBLICATION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/uhc_formulary_operator.py \
  publish-admitted --receipt-id ffur_48_LOWERCASE_HEX_CHARACTERS
```

The publication path imports no downloader or network client. Under a fresh
source lease it reloads the receipt, generic twin admission, exact 48-row
artifact ledger, and retained bytes; recomputes their contract; and only then
uses the existing atomic repository publisher. Missing or corrupt retained
bytes, source drift, admission drift, or predecessor drift leave the pointer
unchanged. Exact replay returns the original generation and timestamp.

## Runtime boundary and acceptance

Neither gate belongs in shared API, worker, scheduler, ConfigMap, or Secret
configuration. Run each phase in a separate one-shot Job with exactly one gate
set to lowercase `true`. Record the exact image digest, migration head, retained
observation hash, receipt, counts, content roots, pointer before/after, storage
reserve, and Job/Pod outcome. A green library test or an admitted receipt is not
publication proof, and publication is not proof that every upstream file was
available unless the retained receipt proves the complete 24+24 census.

The cross-source decisions and binary gates are maintained in
[Provider-directory and formulary source acceptance](provider-directory-formulary-source-acceptance.md).
