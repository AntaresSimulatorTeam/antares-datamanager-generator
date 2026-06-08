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

from antares.datamanager.core.settings import settings
from antares.datamanager.models.study_data_json_model import StudyData
from antares.datamanager.utils.arrow_cleanup_utils import ArrowCleanupUtils


def test_collect_all_arrow_files_comprehensive():
    # Setup mock settings paths
    settings_obj = settings
    load_dir = Path(settings_obj.load_output_directory)
    thermal_dir = Path(settings_obj.param_modulation_directory)
    res_dir = Path(settings_obj.res_ts_directory)
    sts_dir = Path(settings_obj.sts_ts_directory)
    hydro_dir = Path(settings_obj.hydro_ts_directory)
    misc_dir = Path(settings_obj.misc_ts_directory)

    study_data = StudyData(name="test_study")

    # Add various files to study_data
    study_data.area_loads = {"area1": ["load1.arrow", "load2.txt"]}
    study_data.area_thermals = {
        "area1": {"cluster1": {"properties": {"cluster_modulation": ["therm1.arrow", "therm2.csv"]}}}
    }
    study_data.area_res = {
        "area1": {
            "res1": {
                "series": ["res1.arrow"],
                "fr_aggregation": {"series_by_zone_and_tech": {"zone1": {"tech1": "res_agg1.arrow"}}},
            }
        }
    }
    study_data.area_sts = {
        "area1": {"sts1": {"series": ["sts1.arrow"], "stsConstraintsSeriesList": ["sts_const1.arrow"]}}
    }
    study_data.area_hydro = {"area1": {"series": ["hydro1.arrow"]}}
    study_data.area_misc = {
        "area1": {"misc1": {"series": "misc1.arrow"}, "misc2": {"series": ["misc2.arrow", "misc3.txt"]}}
    }

    # Action
    collected_files = ArrowCleanupUtils.collect_all_arrow_files(study_data)

    # Expected files
    expected = {
        (load_dir / "load1.arrow").resolve(),
        (thermal_dir / "therm1.arrow").resolve(),
        (res_dir / "res1.arrow").resolve(),
        (res_dir / "res_agg1.arrow").resolve(),
        (sts_dir / "sts1.arrow").resolve(),
        (sts_dir / "sts_const1.arrow").resolve(),
        (hydro_dir / "hydro1.arrow").resolve(),
        (misc_dir / "misc1.arrow").resolve(),
        (misc_dir / "misc2.arrow").resolve(),
    }

    assert collected_files == expected


def test_collect_all_arrow_files_empty():
    study_data = StudyData(name="empty_study")
    collected_files = ArrowCleanupUtils.collect_all_arrow_files(study_data)
    assert collected_files == set()
