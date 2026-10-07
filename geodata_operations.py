"""Transactional boundaries for HTTP resources and worker checkpoints."""

from __future__ import annotations

import hashlib
import json
from functools import wraps
from typing import Any

from common import json_default, verify_token
from relational_state import StateConflict


def install_operations(handler: Any) -> None:
    names = {function.__name__ for function in handler.routes.values()}
    mutations = {
        function.__name__
        for (method, _), function in handler.routes.items()
        if method != "GET"
    }
    for name in names:
        function = getattr(handler, name)
        write = name in mutations

        def wrap(function=function, write=write):
            @wraps(function)
            def operation(request, params):
                store = handler.store
                if not store.durable:
                    return function(request, params)
                upload = function.__name__ in {
                    "create_import_upload",
                    "upload_import_part",
                    "complete_import_upload",
                    "abort_import_upload",
                    "upload_import",
                }
                try:
                    with store.operation(write=write, atomic=not upload):

                        def execute():
                            version = params.get("If-Match")
                            entity_id = params.get("entityId")
                            if write and version and entity_id:
                                entity = store.items[entity_id]
                                if version.strip('"') != str(
                                    entity["version"]
                                ):
                                    raise StateConflict(
                                        "entity changed; reload before saving"
                                    )
                            return function(request, params)

                        key = params.get("Idempotency-Key")
                        if (
                            write
                            and key
                            and not upload
                            and not store._repository.scope.idempotency_managed
                        ):
                            authorization = params.get("Authorization", "")
                            owner = "anonymous"
                            if authorization.startswith("Bearer "):
                                owner = verify_token(authorization[7:])["sub"]
                            path = params.get("_path", function.__name__)
                            fingerprint = hashlib.sha256(
                                json.dumps(
                                    params.get("_body") or {},
                                    sort_keys=True,
                                    default=json_default,
                                ).encode()
                            ).hexdigest()
                            return store._repository.request_once(
                                f"http:{owner}:{path}:{key}",
                                fingerprint,
                                execute,
                            )
                        return execute()
                except Exception as error:
                    if getattr(error, "sqlstate", None) in {
                        "40P01",
                        "55P03",
                        "23505",
                    }:
                        raise StateConflict(
                            "another request is updating this resource; retry"
                        ) from error
                    raise

            return operation

        setattr(handler, name, staticmethod(wrap()))
    handler.routes = {
        route: getattr(handler, function.__name__)
        for route, function in handler.routes.items()
    }

    for name in (
        "_process_import_run",
        "_recover_import_run",
        "_process_import_queue",
        "_finish_uploaded_import",
        "_resume_pending_upload",
        "_execute_deletion_job",
        "recover_import_runs",
    ):
        function = getattr(handler, name)

        def wrap_worker(function=function):
            @wraps(function)
            def operation(*args, **kwargs):
                with handler.store.operation(write=True, atomic=False):
                    return function(*args, **kwargs)

            return operation

        setattr(handler, name, staticmethod(wrap_worker()))
