"""Endpoint registration kept separate from exact-wave request handlers."""

from __future__ import annotations


def register_control_wave_routes(blueprint):
    """Register exact-wave endpoints and controller lifecycle hooks."""

    from api import control_wave_routes as routes

    blueprint.listener("before_server_start")(
        routes.control_initialize_ptg_wave_receipt_authority
    )
    blueprint.listener("after_server_start")(routes.control_start_ptg_wave_controller)
    blueprint.listener("before_server_stop")(routes.control_stop_ptg_wave_controller)
    blueprint.post("/import-waves")(routes.control_admit_import_wave)
    blueprint.get("/import-waves/<wave_id>")(routes.control_get_import_wave)
    blueprint.get("/import-wave-receipt-key-epochs")(
        routes.control_get_receipt_key_epochs
    )
    blueprint.get("/import-waves/<wave_id>/outcomes")(
        routes.control_get_import_wave_outcomes
    )
    blueprint.post("/import-waves/<wave_id>/linkage-ack")(
        routes.control_record_import_wave_linkage
    )
    blueprint.get("/import-waves/<wave_id>/proof")(
        routes.control_get_import_wave_proof
    )
    blueprint.get(
        "/import-waves/<wave_id>/logical-preclaim-supersession"
    )(routes.control_get_logical_preclaim_supersession)
    blueprint.get(
        "/import-waves/<wave_id>/admission-rollback-supersession"
    )(routes.control_get_admission_rollback_supersession)
    blueprint.get(
        "/import-waves/<wave_id>/materialized-preclaim-supersession"
    )(routes.control_get_materialized_preclaim_supersession)
    blueprint.post(
        "/import-waves/<wave_id>/materialized-preclaim-abandonment"
    )(routes.control_abandon_materialized_preclaim_wave)
    blueprint.get(
        "/import-waves/<wave_id>/materialized-preclaim-abandonment"
    )(routes.control_get_materialized_preclaim_abandonment)
    blueprint.get(
        "/import-waves/<wave_id>/v13-post-ready-failure-abandonment"
    )(routes.control_get_v13_abandonment)
    blueprint.post(
        "/import-waves/<wave_id>/ordinary-terminal-receipts"
    )(routes.control_issue_ordinary_terminal_receipt)
    return blueprint
