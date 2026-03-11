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
from unittest.mock import MagicMock, patch

import pandas as pd

from antares.craft import BindingConstraintFrequency, BindingConstraintOperator
from antares.datamanager.generator.generate_study_process import add_areas_to_study
from antares.datamanager.models.study_data_json_model import StudyData


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
@patch("antares.datamanager.generator.generate_study_process.generate_thermal_clusters")
@patch("antares.datamanager.generator.generate_study_process.generate_sts_clusters")
@patch("antares.datamanager.generator.generate_study_process.generate_dsr_clusters")
def test_add_areas_to_study_creates_binding_constraints(
    mock_generate_dsr,
    mock_generate_sts,
    mock_generate_thermal,
    mock_load_dir,
):
    mock_load_dir.return_value = Path("/tmp")
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj

    # 1. Test Non-FR area
    study_data_non_fr = StudyData(
        name="test_study",
        areas={
            "BE": {
                "dsr": {
                    "BE_DSR": {"properties": {"enabled": True}, "data": {"nb_hour_per_day": 12, "max_hour_per_day": 1}}
                }
            }
        },
    )
    # Mock Step 1 result for BE
    mock_generate_dsr.return_value = pd.DataFrame({"BE_DSR": [100.0] * 365})

    add_areas_to_study(mock_study, study_data_non_fr)

    # Check if create_binding_constraint was called for BE
    mock_study.create_binding_constraint.assert_called_once()
    args, kwargs = mock_study.create_binding_constraint.call_args
    assert kwargs["name"] == "DSR_BE_stock"
    assert kwargs["properties"].time_step == BindingConstraintFrequency.DAILY
    assert kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert kwargs["terms"][0].data.area == "be"
    assert kwargs["terms"][0].data.cluster == "be_dsr 0"
    assert kwargs["less_term_matrix"].shape == (365, 1)
    assert kwargs["less_term_matrix"].iloc[0, 0] == 100.0

    mock_study.create_binding_constraint.reset_mock()
    mock_generate_dsr.reset_mock()

    # 2. Test FR area
    study_data_fr = StudyData(
        name="test_study",
        areas={
            "FR": {
                "dsr": {
                    "FR_DSR_tertiaire": {
                        "properties": {"enabled": True},
                        "data": {"nb_hour_per_day": 13, "max_hour_per_day": 1},
                    }
                }
            }
        },
    )
    # Mock Step 1 result for FR
    mock_generate_dsr.return_value = pd.DataFrame({"FR_DSR_tertiaire": [184.6] * 365})

    add_areas_to_study(mock_study, study_data_fr)

    # Check if create_binding_constraint was called for FR
    mock_study.create_binding_constraint.assert_called_once()
    args, kwargs = mock_study.create_binding_constraint.call_args
    assert kwargs["name"] == "FR_DSR_tertiaire_stock"
    assert kwargs["properties"].time_step == BindingConstraintFrequency.DAILY
    assert kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert kwargs["terms"][0].data.area == "fr"
    assert kwargs["terms"][0].data.cluster == "FR_DSR_tertiaire"
    assert kwargs["less_term_matrix"].shape == (365, 1)
    assert kwargs["less_term_matrix"].iloc[0, 0] == 184.6
