"""Geodata HTTP route maps grouped by catalogue, intake, and job concerns."""

from __future__ import annotations

from typing import Any, Callable

Route = tuple[str, str]
Handler = Callable[..., Any]


def catalogue_routes(handler: Any) -> dict[Route, Handler]:
    return {
        ("GET", "/v1/geodata/adapters"): handler.adapters,
        ("GET", "/v1/geodata/location-options"): handler.location_options,
        ("GET", "/v1/geodata/entities"): handler.list_entities,
        ("GET", "/v1/geodata/entities/{entityId}"): handler.get_entity,
        ("GET", "/v1/geodata/entities/{entityId}/audit"): handler.audit_entity,
        ("GET", "/v1/geodata/tiles/{z}/{x}/{y}"): handler.tile,
        ("GET", "/v1/geodata/conflation"): handler.list_conflation,
        ("POST", "/v1/geodata/proposals"): handler.draw_proposal,
        (
            "POST",
            "/v1/geodata/conflation/{candidateId}/resolve",
        ): handler.resolve_conflation,
        (
            "POST",
            "/v1/geodata/entities/{entityId}/reviews",
        ): handler.review_resource,
        (
            "PUT",
            "/v1/geodata/entities/{entityId}/geometry",
        ): handler.update_geometry,
        (
            "PATCH",
            "/v1/geodata/entities/{entityId}",
        ): handler.patch_entity_metadata,
        (
            "PUT",
            "/v1/geodata/entities/{entityId}/categories",
        ): handler.put_entity_categories,
        (
            "POST",
            "/v1/geodata/entities/{entityId}/location-enrichment-requests",
        ): handler.request_location_enrichment,
    }


def import_routes(handler: Any) -> dict[Route, Handler]:
    return {
        ("GET", "/v1/geodata/imports"): handler.list_imports,
        (
            "GET",
            "/v1/geodata/import-uploads/{uploadId}",
        ): handler.get_import_upload,
        ("GET", "/v1/geodata/imports/{runId}"): handler.get_import,
        (
            "GET",
            "/v1/geodata/imports/{runId}/processing-queues/{queueId}",
        ): handler.get_import_processing_queue,
        (
            "PUT",
            "/v1/geodata/imports/{runId}/cancellation",
        ): handler.cancel_import,
        (
            "GET",
            "/v1/geodata/imports/{runId}/candidates",
        ): handler.list_import_candidates,
        ("POST", "/v1/geodata/imports"): handler.enqueue_import,
        ("POST", "/v1/geodata/import-uploads"): handler.create_import_upload,
        (
            "POST",
            "/v1/geodata/import-uploads/{uploadId}/parts/{partNumber}",
        ): handler.upload_import_part,
        (
            "POST",
            "/v1/geodata/import-uploads/{uploadId}/complete",
        ): handler.complete_import_upload,
        (
            "DELETE",
            "/v1/geodata/import-uploads/{uploadId}",
        ): handler.abort_import_upload,
        (
            "POST",
            "/v1/geodata/imports/{runId}/candidates/validate",
        ): handler.validate_import_candidates,
        (
            "POST",
            "/v1/geodata/imports/{runId}/process",
        ): handler.process_import_candidates,
        (
            "POST",
            "/v1/geodata/imports/{runId}/processed",
        ): handler.mark_import_processed,
        (
            "DELETE",
            "/v1/geodata/load-test-runs/{testRunId}",
        ): handler.cleanup_load_test_run,
    }


def job_routes(handler: Any) -> dict[Route, Handler]:
    return {
        ("GET", "/v1/geodata/refresh-schedules"): handler.list_schedules,
        ("POST", "/v1/geodata/refresh-schedules"): handler.create_schedule,
        (
            "POST",
            "/v1/geodata/refresh-schedules/{scheduleId}/run",
        ): handler.refresh_import,
        (
            "POST",
            "/v1/geodata/entity-deletion-jobs",
        ): handler.create_deletion_job,
        (
            "GET",
            "/v1/geodata/entity-deletion-jobs/{jobId}",
        ): handler.get_deletion_job,
        (
            "POST",
            "/v1/geodata/entity-deletion-jobs/{jobId}/confirm",
        ): handler.confirm_deletion_job,
    }


def build_routes(handler: Any) -> dict[Route, Handler]:
    """Compose the geodata API surface from its cohesive route groups."""
    routes: dict[Route, Handler] = {}
    for route_group in (catalogue_routes, import_routes, job_routes):
        routes.update(route_group(handler))
    return routes
