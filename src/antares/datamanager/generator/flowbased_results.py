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

import csv
import shutil
import tempfile

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

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
                ids.set_axis(range(1, EXPECTED_HOURS + 1), axis=0).to_csv(
                    prepared / "ts.txt",
                    sep=" ",
                    index_label="Date",
                    header=[str(i + 1) for i in range(ids.shape[1])],
                    quoting=csv.QUOTE_NONNUMERIC,
                )
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
