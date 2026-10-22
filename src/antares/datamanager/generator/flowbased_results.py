# Copyright (c) 2024, RTE (https://www.rte-france.com)
#
# See AUTHORS.txt
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# SPDX-License-Identifier: MPL-2.0
#
# This file is part of the Antares project.
from __future__ import annotations

import json
import shutil
import tempfile

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from antares.craft import Month
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import FlowbasedGenerationError
from antares.datamanager.generator.generate_flowbased import EXPECTED_HOURS, FlowbasedAllocation
from antares.datamanager.logs.logging_setup import get_logger

logger = get_logger(__name__)


@contextmanager
def _staging_directory(root: Path, study_name: str) -> Iterator[Path]:
    staging = Path(tempfile.mkdtemp(prefix=f".{study_name}-", dir=root))
    try:
        yield staging
    finally:
        if (staging / "previous").exists():
            logger.error(f"Previous Flow-Based results retained for recovery at {staging / 'previous'}")
        else:
            shutil.rmtree(staging)


@contextmanager
def publish_flowbased_results(
    allocation: FlowbasedAllocation,
    study_name: str,
    study_id: str,
    flowbased_data: dict[str, Any],
    first_month: Month,
) -> Iterator[Path]:
    """Publish an aligned export, restoring the previous export if generation fails."""
    if not study_name or study_name in {".", ".."} or any(char in study_name for char in ("/", "\\", "\0")):
        raise FlowbasedGenerationError(f"Invalid study name for Flow-Based results: {study_name!r}")

    clusters, ids = allocation.clusters, allocation.id_day_types
    if (
        len(ids) != EXPECTED_HOURS
        or ids.shape[1] == 0
        or clusters.shape != ids.shape
        or not clusters.index.equals(ids.index)
        or not clusters.columns.equals(ids.columns)
    ):
        raise FlowbasedGenerationError(
            "Flow-Based allocation matrices must be aligned with 8760 rows and N > 0 columns"
        )

    try:
        root = settings.flowbased_recalculate_output_directory
    except ValueError as exc:
        raise FlowbasedGenerationError(f"Invalid Flow-Based results configuration: {exc}") from exc
    destination = root / study_name
    lock = root / f".{study_name}.lock"
    try:
        root.mkdir(parents=True, exist_ok=True)
        try:
            lock.mkdir()
        except FileExistsError as exc:
            raise FlowbasedGenerationError(
                f"Flow-Based results are already being published for {study_name!r}"
            ) from exc
        try:
            if destination.is_symlink() or (destination.exists() and not destination.is_dir()):
                raise FlowbasedGenerationError(f"Invalid Flow-Based results directory: {destination}")
            with _staging_directory(root, study_name) as staging:
                prepared = staging / "new"
                backup = staging / "previous"
                prepared.mkdir()
                columns = [f"scenario_{i + 1}" for i in range(ids.shape[1])]
                clusters.to_csv(prepared / "clusters.tsv", sep="\t", index=False, header=columns)
                ids.to_csv(prepared / "id_day_types.tsv", sep="\t", index=False, header=columns)
                metadata = {
                    "study_id": study_id,
                    "study_name": study_name,
                    "first_month": first_month.value,
                    "hours": EXPECTED_HOURS,
                    "hour_numbering": "1-based, in study calendar order",
                    "scenarios": columns,
                    "antares_series_indices": list(range(1, ids.shape[1] + 1)),
                    "monte_carlo_series_rule": "(zero_based_MC_year % N) + 1",
                    "ts_path": flowbased_data["ts_path"],
                    "type_days": flowbased_data["type_days"],
                }
                (prepared / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
                if destination.exists():
                    destination.rename(backup)
                published = False
                try:
                    prepared.rename(destination)
                    published = True
                    logger.info(f"Published Flow-Based results: {destination}")
                    yield destination
                except BaseException:
                    if published:
                        shutil.rmtree(destination)
                    if backup.exists():
                        backup.rename(destination)
                    raise
                if backup.exists():
                    shutil.rmtree(backup)
        finally:
            lock.rmdir()
    except OSError as exc:
        raise FlowbasedGenerationError(f"Could not publish Flow-Based results at {destination}: {exc}") from exc
