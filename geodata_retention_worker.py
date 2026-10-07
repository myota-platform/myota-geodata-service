"""Scheduled worker entry point for geodata import retention."""

from __future__ import annotations

import argparse
import logging
import os
import time

from import_retention import purge_expired_imports


def purge_sweep() -> dict[str, object]:
    """Drain all eligible rows in bounded batches, stopping safely on errors."""
    batch_size = int(
        os.environ.get("GEODATA_IMPORT_RETENTION_BATCH_SIZE", "100")
    )
    total = {"eligible": 0, "purged": 0, "failed": 0, "failedRunIds": []}
    excluded: set[str] = set()
    while True:
        result = purge_expired_imports(
            batch_size=batch_size, excluded_run_ids=excluded
        )
        for key in ("eligible", "purged", "failed"):
            total[key] += result[key]
        excluded.update(result["failedRunIds"])
        total["failedRunIds"].extend(result["failedRunIds"])
        if result["eligible"] < batch_size or result["purged"] == 0:
            return total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--loop", action="store_true", help="run now, then repeat daily"
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(
            os.environ.get(
                "GEODATA_IMPORT_RETENTION_INTERVAL_SECONDS", "86400"
            )
        ),
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.interval_seconds < 60:
        parser.error("--interval-seconds must be at least 60")
    while True:
        purge_sweep()
        if not args.loop:
            return
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
