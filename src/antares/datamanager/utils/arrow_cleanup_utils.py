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

from pathlib import Path
from typing import Set

from antares.datamanager.core.settings import settings
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.models.study_data_json_model import StudyData

logger = get_logger(__name__)


class ArrowCleanupUtils:
    @staticmethod
    def cleanup_arrow_files(used_files: Set[Path]) -> None:
        """
        Remove used .arrow files from the output directories after the study generation process.
        """
        for file in used_files:
            if file.exists() and file.name.lower().endswith(".arrow"):
                logger.info(f"Removing arrow file: {file}")
                try:
                    file.unlink()
                except Exception as e:
                    logger.error(f"Failed to remove arrow file {file}: {e}")

    @staticmethod
    def collect_all_arrow_files(study_data: StudyData) -> Set[Path]:
        """
        Collect all .arrow files mentioned in the StudyData.
        """
        all_files: Set[Path] = set()

        # Loads
        load_dir = Path(settings.load_output_directory)
        for loads in study_data.area_loads.values():
            for f in loads:
                if f.strip().lower().endswith(".arrow"):
                    all_files.add((load_dir / f.strip()).resolve())

        # Thermals
        thermal_dir = Path(settings.param_modulation_directory)
        for thermals in study_data.area_thermals.values():
            for thermal_values in thermals.values():
                props = thermal_values.get("properties", {})
                modulation = props.get("cluster_modulation", [])
                for f in modulation:
                    if f.strip().lower().endswith(".arrow"):
                        all_files.add((thermal_dir / f.strip()).resolve())

        # RES
        res_dir = Path(settings.res_ts_directory)
        for res_map in study_data.area_res.values():
            for res_entry in res_map.values():
                # Standard series
                for f in res_entry.get("series", []):
                    if f.strip().lower().endswith(".arrow"):
                        all_files.add((res_dir / f.strip()).resolve())
                # FR aggregation
                fr_agg = res_entry.get("fr_aggregation", {})
                series_by_zone_and_tech = fr_agg.get("series_by_zone_and_tech", {})
                for tech_map in series_by_zone_and_tech.values():
                    for f in tech_map.values():
                        if f.strip().lower().endswith(".arrow"):
                            all_files.add((res_dir / f.strip()).resolve())

        # STS
        sts_dir = Path(settings.sts_ts_directory)
        for sts_map in study_data.area_sts.values():
            for sts_values in sts_map.values():
                for f in sts_values.get("series", []):
                    if f.strip().lower().endswith(".arrow"):
                        all_files.add((sts_dir / f.strip()).resolve())
                for f in sts_values.get("stsConstraintsSeriesList", []):
                    if f.strip().lower().endswith(".arrow"):
                        all_files.add((sts_dir / f.strip()).resolve())

        # Hydro
        hydro_dir = Path(settings.hydro_ts_directory)
        for hydro_values in study_data.area_hydro.values():
            for f in hydro_values.get("series", []):
                if f.strip().lower().endswith(".arrow"):
                    all_files.add((hydro_dir / f.strip()).resolve())

        # Misc
        misc_dir = Path(settings.misc_ts_directory)
        for misc_map in study_data.area_misc.values():
            for group_values in misc_map.values():
                series = group_values.get("series", [])
                if isinstance(series, str):
                    series = [series]
                for f in series:
                    if f.strip().lower().endswith(".arrow"):
                        all_files.add((misc_dir / f.strip()).resolve())

        return all_files
