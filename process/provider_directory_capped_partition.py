# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact leaf census for versioned FHIR servers with capped broad searches."""

import json
import re
import urllib.parse

CONTRACT = "versioned-capped-search-v1"
COUNT_FIELDS = (
    "before_start_pre",
    "after_end_pre",
    "count_pre",
    "before_start_post",
    "after_end_post",
    "count_post",
)
TAIL_FIELDS = ("before_start_pre", "after_end_pre", "before_start_post", "after_end_post")


def search_limit(settings, ceiling):
    """Require an explicit source contract; retain historical config identities."""
    limit = settings.get("versioned_search_limit")
    if limit is not None and (type(limit) is not int or limit <= ceiling):
        raise ValueError("versioned_search_limit_must_exceed_leaf_ceiling")
    return limit


def validate_census(census_by_field):
    """Refuse fabricated totals, incomplete predecessors and foreign proof fields."""
    if not isinstance(census_by_field, dict) or set(census_by_field) != {"contract", "versioned", *COUNT_FIELDS}:
        raise ValueError("capped_census_invalid")
    if census_by_field["contract"] != CONTRACT or census_by_field["versioned"] is not True:
        raise ValueError("capped_census_contract_invalid")
    if any(
        field_value is not None and (type(field_value) is not int or field_value < 0)
        for field_name, field_value in census_by_field.items()
        if field_name in COUNT_FIELDS
    ):
        raise ValueError("capped_census_count_invalid")
    if any(census_by_field[field_name] not in (None, 0) for field_name in TAIL_FIELDS):
        raise ValueError("capped_census_nonempty_tail")
    if census_by_field["count_pre"] is not None and any(
        census_by_field[field_name] != 0 for field_name in TAIL_FIELDS[:2]
    ):
        raise ValueError("capped_census_pre_missing")
    if census_by_field["count_post"] is not None and (
        census_by_field["count_pre"] != census_by_field["count_post"]
        or any(census_by_field[field_name] != 0 for field_name in TAIL_FIELDS)
    ):
        raise ValueError("capped_census_post_mismatch")
    return census_by_field


def complete(census_by_field):
    """Only exact reconciled leaf sums and four empty tails close this census."""
    validate_census(census_by_field)
    return all(census_by_field[field_name] is not None for field_name in COUNT_FIELDS)


def expected_count(census):
    """Keep capped leaf totals distinct from exact unfiltered census totals."""
    return census.capped["count_pre"] if census.capped is not None else census.ranged_root_pre


def enabled(directory, source_record):
    """Select only sources that explicitly opt into versioned capped searches."""
    return any(
        isinstance(config, dict) and config.get("versioned_search_limit") is not None
        for config in directory._last_updated_partition_resource_configs(source_record).values()
    )


def split_observation(directory, source_record, url, status, response_payload, error):
    """Split only opted-in, identified overflows or an actual transport timeout."""
    resource = directory._fhir_collection_search_resource_type(source_record, url)
    config, _ = directory._last_updated_partition_config(source_record, resource)
    if config is None or config.versioned_search_limit is None:
        return None
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    bounds = query.get("_lastUpdated", [])
    if (
        query.get("_summary") != ["count"]
        or len(bounds) != 2
        or {field_value[:2] for field_value in bounds} != {"ge", "lt"}
    ):
        return None
    if (
        status is None
        and error
        and error.split(":", 1)[0]
        in {"TimeoutError", "ServerTimeoutError", "SocketTimeoutError", "ConnectionTimeoutError"}
    ):
        return directory.CountObservation.split("timeout")
    if (
        status != 413
        or error
        or not isinstance(response_payload, dict)
        or response_payload.get("resourceType") != "OperationOutcome"
    ):
        return None
    issues = response_payload.get("issue")
    if not isinstance(issues, list) or len(issues) != 1 or not isinstance(issues[0], dict):
        return None
    issue = issues[0]
    details = issue.get("details")
    if not isinstance(details, dict):
        return None
    match = re.fullmatch(
        r"Search selects more than maximum allowed number of results \(([0-9]+)\)\.", str(details.get("text", ""))
    )
    if (
        issue.get("severity") == "error"
        and issue.get("code") == "too-costly"
        and issue.get("diagnostics") == "<HSFHIRErr>SearchTooCostly"
        and match
        and int(match[1]) == config.versioned_search_limit
    ):
        return directory.CountObservation.overflow(config.versioned_search_limit + 1)
    return None


def _tail_url(directory, state, before):
    parsed = urllib.parse.urlsplit(state.start_url)
    query_items = [
        (field_name, field_value)
        for field_name, field_value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if field_name not in {"_count", "_sort", "_summary", "_total", "_lastUpdated"}
    ]
    root = state.plan.windows["root"]
    boundary = root.start if before else root.end
    query_items.extend(
        (
            ("_summary", "count"),
            ("_total", "accurate"),
            ("_lastUpdated", ("lt" if before else "ge") + directory._format_last_updated_partition_instant(boundary)),
        )
    )
    return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query_items)))


async def _save(directory, resource, config, state):
    await directory._save_last_updated_partition_plan(
        state.context,
        resource,
        config,
        state.plan,
        census=state.census,
        pages_processed=state.pages_fetched,
        rows_processed=state.rows_fetched,
    )


async def _failure(directory, resource, model, config, state, error, count=None):
    if count is not None and count.transient:
        return await directory._saved_partition_retry_result(
            model,
            resource,
            config,
            state,
            error,
            retry_not_before=count.retry_not_before,
        )
    return await directory._saved_partition_terminal_result(model, resource, config, state, error)


async def _check_cancel(directory, options):
    if options.cancel_ctx is not None:
        await directory._raise_if_resource_import_cancelled(options.cancel_ctx, options.cancel_task)


async def _tails(directory, source_record, resource, model, config, state, options, phase):
    for before, stem in ((True, "before_start"), (False, "after_end")):
        field = f"{stem}_{phase}"
        if phase == "pre" and state.census.capped[field] == 0:
            continue
        await _check_cancel(directory, options)
        if state.deadline_at is not None and directory._last_updated_partition_monotonic() >= state.deadline_at:
            return await directory._saved_partition_retry_result(
                model, resource, config, state, "deadline_reached", deadline_reached=True
            )
        count = await directory._fetch_last_updated_partition_count(
            source_record, _tail_url(directory, state, before), timeout=options.timeout
        )
        state.pages_fetched += count.pages_fetched
        observation = count.observation
        if observation is None or observation.kind is not directory.CountKind.EXACT or observation.count != 0:
            return await _failure(directory, resource, model, config, state, f"capped_{field}_not_exact_zero", count)
        state.census.capped[field] = 0
        await _save(directory, resource, config, state)
    return None


def _is_unfiltered_start(directory, source_record, resource, start_url, base):
    """Only an unfiltered anonymous collection can certify endpoint coverage."""
    start = urllib.parse.urlsplit(start_url)
    origin = urllib.parse.urlsplit(base)
    query = urllib.parse.parse_qs(start.query, keep_blank_values=True)
    return not (
        origin.scheme != "https"
        or start.scheme != origin.scheme
        or start.netloc != origin.netloc
        or start.path.rstrip("/") != origin.path.rstrip("/") + "/" + resource
        or start.username is not None
        or start.password is not None
        or start.fragment
        or set(query) - {"_count", "_format"}
        or any(len(values) != 1 for values in query.values())
        or directory._has_source_declared_credentialed_access(source_record)
        or directory._credential_spec_for_source(source_record)
    )


def _supports_versioned_writes(response_payload, resource):
    """R4 versioned writes must assign every stored version a timestamp."""
    rest_blocks = response_payload.get("rest") if isinstance(response_payload, dict) else None
    resources = (
        [
            resource_statement
            for rest in rest_blocks
            if isinstance(rest, dict) and rest.get("mode") == "server" and isinstance(rest.get("resource"), list)
            for resource_statement in rest["resource"]
            if isinstance(resource_statement, dict)
        ]
        if isinstance(rest_blocks, list)
        else []
    )
    matches = [resource_statement for resource_statement in resources if resource_statement.get("type") == resource]
    return not (
        not isinstance(response_payload, dict)
        or response_payload.get("resourceType") != "CapabilityStatement"
        or response_payload.get("fhirVersion") != "4.0.1"
        or len(matches) != 1
        or matches[0].get("versioning") not in {"versioned", "versioned-update"}
        or not isinstance(matches[0].get("interaction"), list)
        or not {"create", "update", "search-type"}.issubset(
            {
                interaction_statement.get("code")
                for interaction_statement in matches[0]["interaction"]
                if isinstance(interaction_statement, dict)
            }
        )
    )


async def _versioned_contract(directory, source_record, resource, model, config, state, options):
    """Use the server's versioned-write contract, never optional generic metadata."""
    await _check_cancel(directory, options)
    if state.deadline_at is not None and directory._last_updated_partition_monotonic() >= state.deadline_at:
        return await directory._saved_partition_retry_result(
            model, resource, config, state, "deadline_reached", deadline_reached=True
        )
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if not _is_unfiltered_start(directory, source_record, resource, state.start_url, base):
        return await _failure(
            directory, resource, model, config, state, "capped_unfiltered_anonymous_contract_required"
        )
    status, response_payload, error, _ = await directory._fetch_source_json(
        source_record, base + "/metadata", timeout=options.timeout
    )
    state.pages_fetched += 1
    if status != 200 or error:
        count = directory.LastUpdatedCountFetch(
            None,
            transient=directory._is_transient_source_fetch_failure(status, error),
            retry_not_before=directory._last_updated_partition_retry_not_before(
                source_record, base + "/metadata", status, response_payload, error
            ),
        )
        return await _failure(
            directory, resource, model, config, state, "capped_versioning_metadata_unavailable", count
        )
    if not _supports_versioned_writes(response_payload, resource):
        return await _failure(directory, resource, model, config, state, "capped_versioned_write_contract_missing")
    return None


async def pre_census(directory, source_record, resource, model, config, state, options):
    """Verify the source contract and both excluded tails before counting leaves."""
    failure = await _versioned_contract(directory, source_record, resource, model, config, state, options)
    if failure is not None:
        return failure
    if state.census.capped is None:
        state.census.capped = {"contract": CONTRACT, "versioned": True, **dict.fromkeys(COUNT_FIELDS)}
    return await _tails(directory, source_record, resource, model, config, state, options, "pre")


async def reconcile(directory, resource, model, config, state):
    """Freeze the sum of authoritative leaf counts without inventing a root total."""
    count = sum(window.count or 0 for window in state.plan.leaf_windows())
    previous = state.census.capped["count_pre"]
    if previous is not None and previous != count:
        return await _failure(directory, resource, model, config, state, "capped_leaf_census_changed")
    state.census.capped["count_pre"] = count
    await _save(directory, resource, config, state)
    return None


async def post_census(directory, source_record, resource, model, config, state, options):
    """Recount every disjoint leaf and both tails before accepting the candidate."""
    total = 0
    for window in state.plan.leaf_windows():
        await _check_cancel(directory, options)
        if state.deadline_at is not None and directory._last_updated_partition_monotonic() >= state.deadline_at:
            return await directory._saved_partition_retry_result(
                model, resource, config, state, "deadline_reached", deadline_reached=True
            )
        url = directory._last_updated_partition_count_url(state, config, window)
        count = await directory._fetch_last_updated_partition_count(source_record, url, timeout=options.timeout)
        state.pages_fetched += count.pages_fetched
        observation = count.observation
        if (
            observation is None
            or observation.kind is not directory.CountKind.EXACT
            or observation.count != window.count
        ):
            return await _failure(directory, resource, model, config, state, "capped_leaf_post_census_mismatch", count)
        total += observation.count
    if total != state.census.capped["count_pre"]:
        return await _failure(directory, resource, model, config, state, "capped_leaf_post_sum_mismatch")
    failure = await _versioned_contract(directory, source_record, resource, model, config, state, options)
    if failure is not None:
        return failure
    failure = await _tails(directory, source_record, resource, model, config, state, options, "post")
    if failure is not None:
        return failure
    state.census.capped["count_post"] = total
    await _save(directory, resource, config, state)
    return None


def restore_census(
    directory,
    control_dict,
    versioned_search_limit=None,
):
    """Restore immutable exact or capped census with matching source contract."""
    census_dict = control_dict.get("census")
    if not isinstance(census_dict, dict):
        raise ValueError("checkpoint census missing")
    count_names = (
        "unfiltered_pre",
        "ranged_root_pre",
        "ranged_root_post",
        "unfiltered_post",
    )
    count_by_name = {count_name: census_dict.get(count_name) for count_name in count_names}
    if any(
        count_value is not None
        and (isinstance(count_value, bool) or not isinstance(count_value, int) or count_value < 0)
        for count_value in count_by_name.values()
    ):
        raise ValueError("checkpoint census count invalid")
    failure = census_dict.get("failure")
    if failure is not None and (not isinstance(failure, str) or not failure):
        raise ValueError("checkpoint census failure invalid")
    if count_by_name["ranged_root_pre"] is not None and count_by_name["unfiltered_pre"] is None:
        raise ValueError("checkpoint ranged pre census lacks unfiltered pre")
    if count_by_name["ranged_root_post"] is not None and count_by_name["ranged_root_pre"] is None:
        raise ValueError("checkpoint ranged post census lacks ranged pre")
    if count_by_name["unfiltered_post"] is not None and count_by_name["ranged_root_post"] is None:
        raise ValueError("checkpoint unfiltered post census lacks ranged post")
    observed_counts = [count_value for count_value in count_by_name.values() if count_value is not None]
    if failure is None and len(set(observed_counts)) > 1:
        raise ValueError("checkpoint census counts conflict without failure")
    capped = census_dict.get("capped")
    if capped is not None:
        if versioned_search_limit is None or observed_counts:
            raise ValueError("checkpoint capped census contract mismatch")
        validate_census(capped)
    elif versioned_search_limit is not None and observed_counts:
        raise ValueError("checkpoint requires capped census")
    return directory.LastUpdatedCompletenessCensus(
        **count_by_name,
        failure=failure,
        capped=capped,
    )


def record_stats(
    directory,
    resource_stats,
    fetch_result,
):
    """Expose exact source proof without fabricating capped root census fields."""
    if fetch_result.fetch_mode != directory.LAST_UPDATED_PARTITION_FETCH_MODE:
        return
    resource_stats["last_updated_partition_sources"] += 1
    completeness_proof = fetch_result.fetch_diagnostic
    capped = completeness_proof.get("capped_search") if isinstance(completeness_proof, dict) else None
    if capped is not None:
        resource_stats["last_updated_capped_sources"] = resource_stats.get("last_updated_capped_sources", 0) + 1
    if not isinstance(completeness_proof, dict) or completeness_proof.get("verified") is not True:
        return
    if capped is not None:
        if not complete(capped):
            return
        for name in ("verified_sources", "versioned_sources"):
            key = f"last_updated_capped_{name}"
            resource_stats[key] = resource_stats.get(key, 0) + 1
        for name in COUNT_FIELDS:
            key = f"last_updated_capped_{name}"
            resource_stats[key] = resource_stats.get(key, 0) + capped[name]
    resource_stats["last_updated_completeness_verified_sources"] += 1
    for proof_field in (
        "unfiltered_pre",
        "ranged_root_pre",
        "exact_leaf_count_sum",
        "pass1_unique",
        "pass2_unique",
        "staged_candidate_count",
        "ranged_root_post",
        "unfiltered_post",
    ):
        proof_value = completeness_proof.get(proof_field)
        if isinstance(proof_value, int) and not isinstance(proof_value, bool):
            resource_stats[f"last_updated_{proof_field}"] += proof_value


def checkpoint_payload(
    directory,
    config,
    plan,
    census=None,
):
    """Persist the original compact checkpoint with optional capped-census evidence."""
    census = census or directory.LastUpdatedCompletenessCensus()
    planner_checkpoint = plan.to_dict()
    control_windows = [
        {
            **{key: field_value for key, field_value in window.items() if key != "passes"},
            "completed_passes": sorted(int(pass_number) for pass_number in window["passes"]),
        }
        for window in planner_checkpoint["windows"]
    ]
    return json.dumps(
        {
            "version": 2,
            "strategy_version": directory.LAST_UPDATED_PARTITION_STRATEGY_VERSION,
            "config": config.identity(),
            "control": {
                "windows": control_windows,
                "failure": planner_checkpoint["failure"],
                "census": {
                    "unfiltered_pre": census.unfiltered_pre,
                    "ranged_root_pre": census.ranged_root_pre,
                    "ranged_root_post": census.ranged_root_post,
                    "unfiltered_post": census.unfiltered_post,
                    "failure": census.failure,
                    **({"capped": census.capped} if census.capped is not None else {}),
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
