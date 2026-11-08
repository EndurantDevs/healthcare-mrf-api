# Managed pricing controls

The managed pricing integration uses protected admission records and immutable
retained input receipts. Configure `HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA` with the
deployment-owned control schema, for example `pricing_control`. The value must
be a lower-case SQL identifier of at most 63 characters. There is no default or
request-selected schema.

The API and each admitted pricing worker must use the same schema and the same
two trusted policy documents. The API reads the fixed files
`/etc/pricing-control/namespace-role-policy.json` and
`/etc/pricing-control/preparation.json`. Worker Jobs receive those files through
the explicitly configured `HLTHPRT_PLAN_PRICING_NAMESPACE_POLICY_CONFIGMAP` and
`HLTHPRT_PLAN_PRICING_PREPARATION_CONFIGMAP` references, using nonoptional keys
with the same filenames in one read-only projected volume. Missing or malformed
managed configuration refuses the operation. Requests outside this integration
do not require these settings.

Only the exact supported bulk schema revision activates managed admission.
Supported historical revisions keep their existing serialization behavior.
Managed requests bypass importer-wide serialization only after their complete
request and exact protected dispatch membership have been authenticated. The
actual insertion, launch and initial worker claim recheck admission under the
same per-attempt control fence. These checks use the committed aggregate input
seal and closed native custody, without repeating source relationship checks.

The existing authenticated import GET accepts the optional single selector
`evidence=managed-pricing-no-work-v1`. It returns bounded native row and lineage
hashes plus fresh terminal worker and queue observations. Pending, unknown or
missing enqueue settlement returns a conflict and retains inputs. Unknown
producer crashes are not automatically reconciled by this endpoint.

A queue census describes an instant. Safety after the response depends on
closed admission guards rejecting every later delivery, retry, launch and
initial claim before target code can read or write inputs. Activation therefore
requires all affected writers to use the guarded source and old unguarded
workers and API producers to be drained. The endpoint does not remove pins or
modify queue or worker state.
