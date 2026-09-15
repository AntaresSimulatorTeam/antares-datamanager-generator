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

import pytest

from unittest.mock import MagicMock

import numpy as np

from antares.craft import Study, TransmissionCapacities
from antares.datamanager.exceptions.exceptions import MEGenerationError
from antares.datamanager.generator.generate_me import (
    EXPECTED_HOURS,
    _constant_hurdle_cost_df,
    add_me_areas_to_study,
    add_me_links_to_study,
    generate_me,
)


def test_add_me_areas_to_study_creates_areas_with_properties():
    study = MagicMock(spec=Study)
    area_me = {
        "V_ME_H2_LONG_FR": {
            "properties": {"energy_cost_unsupplied": 5376, "energy_cost_spilled": 0, "adequacy_patch_mode": "inside"},
            "ui": "AreaUI class as JSON",
        },
        "V_ME_H2_SHORT_FR": {},
    }

    add_me_areas_to_study(study, area_me)

    assert study.create_area.call_count == 2
    calls_by_name = {call.kwargs["area_name"]: call.kwargs for call in study.create_area.call_args_list}
    assert set(calls_by_name) == {"V_ME_H2_LONG_FR", "V_ME_H2_SHORT_FR"}

    props = calls_by_name["V_ME_H2_LONG_FR"]["properties"]
    assert props.energy_cost_unsupplied == 5376
    assert props.energy_cost_spilled == 0
    assert props.adequacy_patch_mode == "inside"
    assert calls_by_name["V_ME_H2_LONG_FR"]["ui"] is not None


def test_add_me_areas_to_study_wraps_error():
    study = MagicMock(spec=Study)
    study.create_area.side_effect = Exception("backend failed")

    with pytest.raises(MEGenerationError, match="V_ME_H2_LONG_FR"):
        add_me_areas_to_study(study, {"V_ME_H2_LONG_FR": {}})


@pytest.mark.parametrize(
    ("direct", "indirect", "expected_col0", "expected_col1"),
    [
        (0.1, 0.2, 0.1, 0.2),
        (None, None, 0.0, 0.0),
    ],
)
def test_constant_hurdle_cost_df(direct, indirect, expected_col0, expected_col1):
    df = _constant_hurdle_cost_df(direct, indirect)
    assert df.shape == (EXPECTED_HOURS, 6)
    assert np.all(df.iloc[:, 0].to_numpy() == expected_col0)
    assert np.all(df.iloc[:, 1].to_numpy() == expected_col1)
    assert np.all(df.iloc[:, 2:].to_numpy() == 0.0)


def test_add_me_links_to_study_finite_capacities():
    study = MagicMock(spec=Study)
    mock_link = MagicMock()
    study.create_link.return_value = mock_link

    links_me = {
        "v_me_h2_long_euest/v_me_h2_long_iber": {
            "directMw": 12000,
            "indirectMw": 12000,
            "hurdleCostDirect": 0.1,
            "hurdleCostIndirect": 0.1,
        },
    }

    add_me_links_to_study(study, links_me)

    study.create_link.assert_called_once()
    _, kwargs = study.create_link.call_args
    assert kwargs["area_from"] == "v_me_h2_long_euest"
    assert kwargs["area_to"] == "v_me_h2_long_iber"
    assert kwargs["properties"].transmission_capacities == TransmissionCapacities.ENABLED

    mock_link.set_capacity_direct.assert_called_once()
    mock_link.set_capacity_indirect.assert_called_once()
    assert np.all(mock_link.set_capacity_direct.call_args[0][0].to_numpy() == 12000)
    assert np.all(mock_link.set_capacity_indirect.call_args[0][0].to_numpy() == 12000)

    assert mock_link.update_properties.call_args[0][0].hurdles_cost is True
    params_df = mock_link.set_parameters.call_args[0][0]
    assert params_df.iloc[0, 0] == 0.1
    assert params_df.iloc[0, 1] == 0.1


def test_add_me_links_to_study_both_infinite():
    study = MagicMock(spec=Study)
    mock_link = MagicMock()
    study.create_link.return_value = mock_link

    links_me = {
        "FR/z_p2g_long_fr": {"directMw": None, "indirectMw": None, "hurdleCostDirect": 0, "hurdleCostIndirect": 0},
    }

    add_me_links_to_study(study, links_me)

    _, kwargs = study.create_link.call_args
    assert kwargs["properties"].transmission_capacities == TransmissionCapacities.INFINITE
    mock_link.set_capacity_direct.assert_not_called()
    mock_link.set_capacity_indirect.assert_not_called()
    # hurdle cost always true
    assert mock_link.update_properties.call_args[0][0].hurdles_cost is True


def test_add_me_links_to_study_mixed_infinite_raises():
    study = MagicMock(spec=Study)

    links_me = {"FR/z_p2g_long_fr": {"directMw": 6280, "indirectMw": None}}

    with pytest.raises(MEGenerationError, match="directMw and indirectMw"):
        add_me_links_to_study(study, links_me)

    study.create_link.assert_not_called()


def test_add_me_links_to_study_wraps_error():
    study = MagicMock(spec=Study)
    study.create_link.side_effect = Exception("link failed")

    with pytest.raises(MEGenerationError, match="fr/be"):
        add_me_links_to_study(study, {"FR/BE": {"directMw": 100, "indirectMw": 100}})


def test_generate_me_handles_missing_sections():
    # backend removes "area_me"/"links_me" completely when empty, (or me itself)
    study = MagicMock(spec=Study)
    generate_me(study, {})
    study.create_area.assert_not_called()
    study.create_link.assert_not_called()
