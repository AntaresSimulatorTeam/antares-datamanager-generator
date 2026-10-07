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

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from antares.craft import (
    BindingConstraintFrequency,
    BindingConstraintOperator,
    ClusterData,
    LinkData,
    Study,
    TransmissionCapacities,
)
from antares.datamanager.exceptions.exceptions import MEGenerationError
from antares.datamanager.generator.generate_me import (
    EXPECTED_HOURS,
    _constant_hurdle_cost_df,
    add_me_areas_to_study,
    add_me_g2p_binding_constraints,
    add_me_hydro_to_study,
    add_me_links_to_study,
    add_me_p2g_binding_constraints,
    add_me_sts_to_study,
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

    with patch("antares.datamanager.generator.generate_me.settings") as mock_settings:
        mock_settings.load_output_directory = Path("/fake/load/dir")
        area_objs = add_me_areas_to_study(study, area_me, used_files=set())

    assert study.create_area.call_count == 2
    calls_by_name = {call.kwargs["area_name"]: call.kwargs for call in study.create_area.call_args_list}
    assert set(calls_by_name) == {"V_ME_H2_LONG_FR", "V_ME_H2_SHORT_FR"}
    assert set(area_objs) == {"V_ME_H2_LONG_FR", "V_ME_H2_SHORT_FR"}

    props = calls_by_name["V_ME_H2_LONG_FR"]["properties"]
    assert props.energy_cost_unsupplied == 5376
    assert props.energy_cost_spilled == 0
    assert props.adequacy_patch_mode == "inside"
    assert calls_by_name["V_ME_H2_LONG_FR"]["ui"] is not None


def test_add_me_areas_to_study_wraps_error():
    study = MagicMock(spec=Study)
    study.create_area.side_effect = Exception("backend failed")

    with patch("antares.datamanager.generator.generate_me.settings") as mock_settings:
        mock_settings.load_output_directory = Path("/fake/load/dir")
        with pytest.raises(MEGenerationError, match="V_ME_H2_LONG_FR"):
            add_me_areas_to_study(study, {"V_ME_H2_LONG_FR": {}}, used_files=set())


def test_add_me_areas_to_study_applies_loads():
    study = MagicMock(spec=Study)
    mock_area_obj = MagicMock()
    study.create_area.return_value = mock_area_obj
    used_files: set[Path] = set()

    area_me = {
        "V_ME_H2_LONG_FR": {"loads": ["load_v_me_h2_long_fr_2026-2027.csv.uuid.arrow"]},
        "V_ME_H2_SHORT_FR": {"loads": "No LOAD files for this area"},
    }

    with (
        patch("antares.datamanager.generator.generate_me.settings") as mock_settings,
        patch("antares.datamanager.generator.generate_me.pd.read_feather") as mock_read_feather,
    ):
        mock_settings.load_output_directory = Path("/fake/load/dir")
        mock_read_feather.return_value = "fake_df"
        add_me_areas_to_study(study, area_me, used_files)

    mock_read_feather.assert_called_once_with(Path("/fake/load/dir/load_v_me_h2_long_fr_2026-2027.csv.uuid.arrow"))
    mock_area_obj.set_load.assert_called_once_with("fake_df")
    assert used_files == {Path("/fake/load/dir/load_v_me_h2_long_fr_2026-2027.csv.uuid.arrow")}


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
    generate_me(study, {}, used_files=set())
    study.create_area.assert_not_called()
    study.create_link.assert_not_called()


@pytest.mark.parametrize(
    "binding_data",
    [
        {},
        {"binding_constraints_me": None},
        {"binding_constraints_me": {}},
        {"binding_constraints_me": {"constraints_P2G": None, "constraints_G2P": None}},
        {"binding_constraints_me": {"constraints_P2G": [], "constraints_G2P": []}},
        {"binding_constraints_me": {"constraints_P2G": [{"node": "z_p2g_long_fr", "efficiency": 0.744}]}},
        {"binding_constraints_me": {"constraints_G2P": [{"name": "g2p_fr"}]}},
        {
            "binding_constraints_me": {
                "constraints_P2G": [{"node": "z_p2g_long_fr", "efficiency": 0.744}],
                "constraints_G2P": [{"name": "g2p_fr"}],
            }
        },
    ],
)
def test_generate_me_calls_only_nonempty_binding_constraints(binding_data):
    study = MagicMock(spec=Study)
    links_me = {"FR/z_p2g_long_fr": {"directMw": 100, "indirectMw": 100}}
    me_data = {"area_me": {"z_p2g_long_fr": {}}, "links_me": links_me, **binding_data}

    with (
        patch("antares.datamanager.generator.generate_me.settings") as mock_settings,
        patch("antares.datamanager.generator.generate_me.add_me_p2g_binding_constraints") as mock_p2g,
        patch("antares.datamanager.generator.generate_me.add_me_g2p_binding_constraints") as mock_g2p,
    ):
        mock_settings.load_output_directory = Path("/fake/load/dir")
        generate_me(study, me_data, used_files=set())

    constraints = binding_data.get("binding_constraints_me") or {}
    if constraints.get("constraints_P2G"):
        mock_p2g.assert_called_once_with(study, links_me, constraints["constraints_P2G"])
    else:
        mock_p2g.assert_not_called()
    if constraints.get("constraints_G2P"):
        mock_g2p.assert_called_once_with(study, constraints["constraints_G2P"])
    else:
        mock_g2p.assert_not_called()
    study.create_area.assert_called_once()
    study.create_link.assert_called_once()


@pytest.mark.parametrize(
    ("generating_daily", "pumping_daily"),
    [(False, False), (True, False), (False, True), (True, True)],
)
def test_generate_me_hydro_maxpower_modes(tmp_path, monkeypatch, generating_daily, pumping_daily):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    study = MagicMock(spec=Study)
    area = study.create_area.return_value
    used_files: set[Path] = set()
    hydro_def = {
        "properties": {"reservoir_capacity": 72000000, "use_heuristic": False},
        "inflow_structure": {"intermonthly_correlation": 0.5},
        "allocation": {"AT": 1},
        "generating_pmax": {"pmax": 100, "hours": 12},
        "pumping_pmax": {"pmax": 200, "hours": 6},
    }
    for side, daily in (("generating", generating_daily), ("pumping", pumping_daily)):
        if daily:
            file_name = f"{side}.arrow"
            pd.DataFrame({"V_ME_H2_LONG_FR": list(range(365)), "other": [999] * 365}).to_feather(tmp_path / file_name)
            hydro_def[f"{side}_series"] = file_name

    generate_me(
        study,
        {"area_me": {"V_ME_H2_LONG_FR": {}}, "hydro_me": {"v_me_h2_long_fr": hydro_def}},
        used_files,
    )

    assert area.hydro.update_properties.call_args.args[0].reservoir_capacity == 72000000
    assert area.hydro.update_properties.call_args.args[0].use_heuristic is False
    assert area.hydro.update_inflow_structure.call_args.args[0].intermonthly_correlation == 0.5
    area.hydro.set_allocation.assert_called_once()
    assert area.hydro.set_allocation.call_args.args[0][0].area_id == "at"
    assert area.hydro.set_allocation.call_args.args[0][0].coefficient == 1
    maxpower = area.hydro.set_maxpower.call_args.args[0]
    assert maxpower.shape == (365, 4)
    assert list(maxpower.columns) == ["0", "1", "2", "3"]
    for side, daily, column, annual_value, hours in (
        ("generating", generating_daily, "0", 100, 12),
        ("pumping", pumping_daily, "2", 200, 6),
    ):
        assert maxpower[column].tolist() == (list(range(365)) if daily else [annual_value] * 365)
        assert (maxpower["1" if side == "generating" else "3"] == hours).all()
    assert used_files == {
        tmp_path / f"{side}.arrow"
        for side, daily in (("generating", generating_daily), ("pumping", pumping_daily))
        if daily
    }


def test_generate_me_hydro_generating_only_defaults_pumping(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    pd.DataFrame({"v_me_h2_long_fr": [7] * 365}).to_feather(tmp_path / "generating.arrow")
    study = MagicMock(spec=Study)

    generate_me(
        study,
        {
            "area_me": {"v_me_h2_long_fr": {}},
            "hydro_me": {"v_me_h2_long_fr": {"generating_series": "generating.arrow"}},
        },
        set(),
    )

    maxpower = study.create_area.return_value.hydro.set_maxpower.call_args.args[0]
    assert (maxpower["0"] == 7).all()
    assert (maxpower["1"] == 24).all()
    assert (maxpower["2"] == 0).all()
    assert (maxpower["3"] == 24).all()


@pytest.mark.parametrize("failure", ["missing_area", "missing_file", "missing_node", "wrong_length"])
def test_generate_me_hydro_invalid_input(tmp_path, monkeypatch, failure):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    area_objs = {} if failure == "missing_area" else {"v_me_h2_long_fr": MagicMock()}
    hydro_def = {"generating_series": "generating.arrow"}
    if failure in ("missing_node", "wrong_length"):
        pd.DataFrame(
            {
                "other" if failure == "missing_node" else "v_me_h2_long_fr": [1]
                * (364 if failure == "wrong_length" else 365)
            }
        ).to_feather(tmp_path / "generating.arrow")

    with pytest.raises(MEGenerationError, match="v_me_h2_long_fr"):
        add_me_hydro_to_study(area_objs, {"v_me_h2_long_fr": hydro_def}, set())

    if area_objs:
        area_objs["v_me_h2_long_fr"].hydro.set_maxpower.assert_not_called()


def test_generate_me_hydro_reservoir_ts(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    file_name = "v_me_h2_long_fr_reservoir_levels_248d8fd2802d86348ce799154cacd252.arrow"
    pd.DataFrame({"Minimum": [0.1] * 365, "Moyen": [0.5] * 365, "Maximum": [0.9] * 365}).to_feather(
        tmp_path / file_name
    )
    area = MagicMock()
    used_files: set[Path] = set()

    add_me_hydro_to_study({"v_me_h2_long_fr": area}, {"v_me_h2_long_fr": {"reservoir_ts": file_name}}, used_files)

    reservoir = area.hydro.set_reservoir.call_args.args[0]
    assert reservoir.shape == (365, 3)
    assert list(reservoir.columns) == [0, 1, 2]
    assert reservoir.iloc[0].tolist() == [0.1, 0.5, 0.9]
    assert tmp_path / file_name in used_files


@pytest.mark.parametrize("failure", ["missing_file", "wrong_shape", "not_a_name"])
def test_generate_me_hydro_reservoir_ts_invalid(tmp_path, monkeypatch, failure):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    reservoir_ts: object = "reservoir.arrow"
    if failure == "wrong_shape":
        pd.DataFrame({"Minimum": [0.1] * 365, "Moyen": [0.5] * 365}).to_feather(tmp_path / "reservoir.arrow")
    elif failure == "not_a_name":
        reservoir_ts = 42
    area = MagicMock()

    with pytest.raises(MEGenerationError, match="v_me_h2_long_fr"):
        add_me_hydro_to_study({"v_me_h2_long_fr": area}, {"v_me_h2_long_fr": {"reservoir_ts": reservoir_ts}}, set())

    area.hydro.set_reservoir.assert_not_called()


def test_generate_me_hydro_timeseries_ts(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    pd.DataFrame({"ts1": [1.0] * 8760, "ts2": [2.0] * 8760}).to_feather(tmp_path / "n_ror.abc.arrow")
    pd.DataFrame({"ts1": [3.0] * 365}).to_feather(tmp_path / "n_mod.abc.arrow")
    area = MagicMock()
    used_files: set[Path] = set()

    add_me_hydro_to_study(
        {"n": area},
        {"n": {"timeseries_ts": {"ror": "n_ror.abc.arrow", "mod": "n_mod.abc.arrow"}}},
        used_files,
    )

    ror = area.hydro.set_ror_series.call_args.args[0]
    mod = area.hydro.set_mod_series.call_args.args[0]
    assert ror.shape == (8760, 2) and ror.iloc[0].tolist() == [1.0, 2.0]
    assert mod.shape == (365, 1) and (mod[0] == 3.0).all()
    assert used_files == {tmp_path / "n_ror.abc.arrow", tmp_path / "n_mod.abc.arrow"}


def test_generate_me_hydro_timeseries_ts_without_headers_keeps_all_data(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    ror_values = np.arange(8760 * 3, dtype=float).reshape(8760, 3)
    mod_values = np.arange(365 * 2, dtype=float).reshape(365, 2)
    pd.DataFrame(ror_values).rename(columns=str).to_feather(tmp_path / "n_ror.arrow")
    pd.DataFrame(mod_values).rename(columns=str).to_feather(tmp_path / "n_mod.arrow")
    area = MagicMock()

    add_me_hydro_to_study({"n": area}, {"n": {"timeseries_ts": {"ror": "n_ror.arrow", "mod": "n_mod.arrow"}}}, set())

    np.testing.assert_array_equal(area.hydro.set_ror_series.call_args.args[0].to_numpy(), ror_values)
    np.testing.assert_array_equal(area.hydro.set_mod_series.call_args.args[0].to_numpy(), mod_values)


@pytest.mark.parametrize("failure", ["unknown_key", "missing_file", "wrong_rows", "not_an_object"])
def test_generate_me_hydro_timeseries_ts_invalid(tmp_path, monkeypatch, failure):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    timeseries_ts: object = {"ror": "n_ror.arrow"}
    if failure == "unknown_key":
        timeseries_ts = {"inflow": "n_ror.arrow"}
    elif failure == "wrong_rows":
        pd.DataFrame({"ts1": [1.0] * 365}).to_feather(tmp_path / "n_ror.arrow")
    elif failure == "not_an_object":
        timeseries_ts = "n_ror.arrow"
    area = MagicMock()

    with pytest.raises(MEGenerationError, match="ME hydro n"):
        add_me_hydro_to_study({"n": area}, {"n": {"timeseries_ts": timeseries_ts}}, set())

    area.hydro.set_ror_series.assert_not_called()


def test_generate_me_hydro_water_values_ts(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    values = np.arange(365 * 101, dtype=float).reshape(365, 101)
    file_name = "v_me_h2_long_euest_f46b2a3b.arrow"
    pd.DataFrame(values, columns=[f"Column{i}" for i in range(101)]).to_feather(tmp_path / file_name)
    area = MagicMock()
    used_files: set[Path] = set()

    add_me_hydro_to_study({"n": area}, {"n": {"water_values_ts": file_name}}, used_files)

    np.testing.assert_array_equal(area.hydro.set_water_values.call_args.args[0].to_numpy(), values)
    assert used_files == {tmp_path / file_name}


@pytest.mark.parametrize("shape", [(365, 100), (364, 101)])
def test_generate_me_hydro_water_values_ts_wrong_shape(tmp_path, monkeypatch, shape):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_ME_OUTPUT_DIRECTORY", str(tmp_path))
    pd.DataFrame(np.zeros(shape)).rename(columns=str).to_feather(tmp_path / "wv.arrow")
    area = MagicMock()

    with pytest.raises(MEGenerationError, match="ME hydro n: water_values_ts"):
        add_me_hydro_to_study({"n": area}, {"n": {"water_values_ts": "wv.arrow"}}, set())

    area.hydro.set_water_values.assert_not_called()


def test_add_me_sts_to_study_creates_clusters():
    mock_area_obj = MagicMock()
    area_objs = {"V_ME_H2_SHORT_FR": mock_area_obj}
    sts_me = {
        "V_ME_H2_SHORT_FR_st_storage": {
            "properties": {"enabled": True, "group": "other1"},
            "series": ["lower_curve.xlsx.uuid.arrow"],
        }
    }
    area_me = {"V_ME_H2_SHORT_FR": {"sts_me": sts_me}}
    used_files: set[Path] = set()

    with patch("antares.datamanager.generator.generate_me.generate_sts_clusters") as mock_generate_sts:
        add_me_sts_to_study(area_objs, area_me, used_files)

    mock_generate_sts.assert_called_once_with(mock_area_obj, sts_me, used_files)


def test_add_me_sts_to_study_skips_areas_without_sts():
    area_objs = {"V_ME_H2_LONG_FR": MagicMock()}
    area_me = {
        "V_ME_H2_LONG_FR": {},
        "V_ME_H2_SHORT_FR": {"sts_me": {}},
        "V_ME_H2_OTHER_FR": {"sts_me": "not a dict"},
    }

    with patch("antares.datamanager.generator.generate_me.generate_sts_clusters") as mock_generate_sts:
        add_me_sts_to_study(area_objs, area_me, used_files=set())

    mock_generate_sts.assert_not_called()


def test_add_me_sts_to_study_wraps_error():
    mock_area_obj = MagicMock()
    area_objs = {"V_ME_H2_SHORT_FR": mock_area_obj}
    area_me = {"V_ME_H2_SHORT_FR": {"sts_me": {"cluster": {"properties": {}}}}}

    with patch("antares.datamanager.generator.generate_me.generate_sts_clusters") as mock_generate_sts:
        mock_generate_sts.side_effect = Exception("sts failed")
        with pytest.raises(MEGenerationError, match="V_ME_H2_SHORT_FR"):
            add_me_sts_to_study(area_objs, area_me, used_files=set())


def test_generate_me_creates_sts_after_areas_and_links():
    study = MagicMock(spec=Study)
    mock_area_obj = MagicMock()
    study.create_area.return_value = mock_area_obj
    sts_me = {"V_ME_H2_SHORT_FR_st_storage": {"properties": {}, "series": []}}
    me_data = {
        "area_me": {"V_ME_H2_SHORT_FR": {"sts_me": sts_me}},
        "links_me": {"v_me_h2_long_euest/v_me_h2_long_iber": {"directMw": None, "indirectMw": None}},
    }
    call_order: list[str] = []
    study.create_area.side_effect = lambda **_: call_order.append("area") or mock_area_obj
    study.create_link.side_effect = lambda **_: call_order.append("link") or MagicMock()

    with (
        patch("antares.datamanager.generator.generate_me.settings") as mock_settings,
        patch("antares.datamanager.generator.generate_me.generate_sts_clusters") as mock_generate_sts,
    ):
        mock_settings.load_output_directory = Path("/fake/load/dir")
        mock_generate_sts.side_effect = lambda *_args, **_kwargs: call_order.append("sts")
        generate_me(study, me_data, used_files=set())

    assert call_order == ["area", "link", "sts"]
    mock_generate_sts.assert_called_once_with(mock_area_obj, sts_me, set())


def test_generate_me_creates_p2g_binding_constraints_after_sts():
    study = MagicMock(spec=Study)
    mock_area_obj = MagicMock()
    study.create_area.return_value = mock_area_obj
    me_data = {
        "area_me": {"V_ME_H2_SHORT_FR": {}},
        "links_me": {"FR/z_p2g_short_fr": {"directMw": 100, "indirectMw": 100}},
        "binding_constraints_me": {"constraints_P2G": [{"node": "z_p2g_short_fr", "efficiency": 0.744}]},
    }
    call_order: list[str] = []
    study.create_area.side_effect = lambda **_: call_order.append("area") or mock_area_obj
    study.create_link.side_effect = lambda **_: call_order.append("link") or MagicMock()
    study.create_binding_constraint.side_effect = lambda **_: call_order.append("bc") or MagicMock()

    with patch("antares.datamanager.generator.generate_me.settings") as mock_settings:
        mock_settings.load_output_directory = Path("/fake/load/dir")
        generate_me(study, me_data, used_files=set())

    assert call_order == ["area", "link", "bc"]


def test_add_me_p2g_binding_constraints_mixed_me_and_elec_links():
    study = MagicMock(spec=Study)
    links_me = {
        "v_me_h2_short_fr/z_p2g_short_fr": {"directMw": None, "indirectMw": None},
        "FR/z_p2g_short_fr": {"directMw": 100, "indirectMw": 100},
    }
    constraints_p2g = [{"node": "z_p2g_short_fr", "efficiency": 0.744}]

    add_me_p2g_binding_constraints(study, links_me, constraints_p2g)

    study.create_binding_constraint.assert_called_once()
    kwargs = study.create_binding_constraint.call_args.kwargs
    assert kwargs["name"] == "efficiency_z_p2g_short_fr"
    assert kwargs["properties"].operator == BindingConstraintOperator.EQUAL
    assert "equal_term_matrix" not in kwargs

    weights_by_term_id = {term.id: term.weight for term in kwargs["terms"]}
    assert weights_by_term_id["v_me_h2_short_fr%z_p2g_short_fr"] == 1.0
    assert weights_by_term_id["fr%z_p2g_short_fr"] == 0.744


def test_add_me_p2g_binding_constraints_skips_nodes_without_links():
    study = MagicMock(spec=Study)
    links_me = {"FR/BE": {"directMw": 100, "indirectMw": 100}}
    constraints_p2g = [{"node": "z_p2g_short_fr", "efficiency": 0.744}]

    add_me_p2g_binding_constraints(study, links_me, constraints_p2g)

    study.create_binding_constraint.assert_not_called()


@pytest.mark.parametrize(
    "constraints_p2g",
    [
        [{"efficiency": 0.744}],  # missing node
        [{"node": "z_p2g_short_fr"}],  # missing efficiency
        [{"node": "z_p2g_short_fr", "efficiency": "not_a_number"}],
    ],
    ids=["missing_node", "missing_efficiency", "non_numeric_efficiency"],
)
def test_add_me_p2g_binding_constraints_invalid_entry_raises(constraints_p2g):
    study = MagicMock(spec=Study)
    links_me = {"FR/z_p2g_short_fr": {"directMw": 100, "indirectMw": 100}}

    with pytest.raises(MEGenerationError, match="Invalid P2G efficiency entry"):
        add_me_p2g_binding_constraints(study, links_me, constraints_p2g)


@pytest.mark.parametrize(
    "links_me",
    [
        {"FR/z_p2g_short_fr": {"directMw": 100, "indirectMw": 100}},  # ELEC<->ME link
        {"v_me_h2_short_fr/z_p2g_short_fr": {"directMw": None, "indirectMw": None}},  # ME<->ME link only
    ],
    ids=["elec_link", "me_only_link"],
)
def test_add_me_p2g_binding_constraints_missing_efficiency_raises(links_me):
    study = MagicMock(spec=Study)

    with pytest.raises(MEGenerationError, match="z_p2g_short_fr"):
        add_me_p2g_binding_constraints(study, links_me, constraints_p2g=[])

    study.create_binding_constraint.assert_not_called()

    study.create_binding_constraint.assert_not_called()


def test_add_me_p2g_binding_constraints_case_insensitive():
    study = MagicMock(spec=Study)
    links_me = {"FR/Z_P2G_SHORT_FR": {"directMw": 100, "indirectMw": 100}}
    constraints_p2g = [{"node": "Z_P2G_SHORT_FR", "efficiency": 0.744}]

    add_me_p2g_binding_constraints(study, links_me, constraints_p2g)

    kwargs = study.create_binding_constraint.call_args.kwargs
    weights_by_term_id = {term.id: term.weight for term in kwargs["terms"]}
    assert weights_by_term_id["fr%z_p2g_short_fr"] == 0.744


def test_add_me_p2g_binding_constraints_wraps_unexpected_errors():
    study = MagicMock(spec=Study)
    study.create_binding_constraint.side_effect = Exception("backend failed")
    links_me = {"v_me_h2_short_fr/z_p2g_short_fr": {"directMw": None, "indirectMw": None}}
    constraints_p2g = [{"node": "z_p2g_short_fr", "efficiency": 0.744}]

    with pytest.raises(MEGenerationError, match="z_p2g_short_fr"):
        add_me_p2g_binding_constraints(study, links_me, constraints_p2g)


def test_add_me_g2p_binding_constraints_creates_link_and_cluster_terms():
    study = MagicMock(spec=Study)
    constraints_g2p = [
        {
            "name": "g2p_euest",
            "enabled": True,
            "type": "hourly",
            "operator": "equal",
            "node_1_left": "v_me_h2_long_euest",
            "node_2_left": "z_ME_consoElec",
            "node_right_area": {
                "AT": {"CCGT H2 pcomp": {"efficiency": 51}},
                "BE": {},
                "DE": {"CCGT H2 pcomp": {"efficiency": 51}},
            },
        }
    ]

    add_me_g2p_binding_constraints(study, constraints_g2p)

    study.create_binding_constraint.assert_called_once()
    kwargs = study.create_binding_constraint.call_args.kwargs
    assert kwargs["name"] == "g2p_euest"
    assert kwargs["properties"].enabled is True
    assert kwargs["properties"].time_step == BindingConstraintFrequency.HOURLY
    assert kwargs["properties"].operator == BindingConstraintOperator.EQUAL

    link_term = next(term for term in kwargs["terms"] if isinstance(term.data, LinkData))
    assert link_term.data == LinkData(area1="v_me_h2_long_euest", area2="z_me_consoelec")
    assert link_term.weight == 1.0

    cluster_terms = [term for term in kwargs["terms"] if isinstance(term.data, ClusterData)]
    assert {(term.data.area, term.data.cluster) for term in cluster_terms} == {
        ("at", "at_ccgt h2 pcomp"),
        ("de", "de_ccgt h2 pcomp"),
    }
    assert all(term.weight == pytest.approx(-1 / 51) for term in cluster_terms)


def test_generate_me_creates_g2p_binding_constraint():
    study = MagicMock(spec=Study)
    constraint = {
        "name": "g2p_euest",
        "enabled": True,
        "type": "hourly",
        "operator": "equal",
        "node_1_left": "v_me_h2_long_euest",
        "node_2_left": "z_me_consoelec",
        "node_right_area": {},
    }

    generate_me(
        study,
        {"binding_constraints_me": {"constraints_G2P": [constraint]}},
        used_files=set(),
    )

    assert study.create_binding_constraint.call_args.kwargs["name"] == "g2p_euest"


@pytest.mark.parametrize(
    ("constraint_update", "error_match"),
    [
        ({"node_1_left": ""}, "node_1_left"),
        ({"type": "monthly"}, "monthly"),
        ({"operator": "invalid"}, "invalid"),
        ({"node_right_area": {"AT": {"cluster": {"efficiency": 0}}}}, "must not be zero"),
        ({"node_right_area": {"AT": {"cluster": {}}}}, "efficiency"),
    ],
)
def test_add_me_g2p_binding_constraints_rejects_invalid_data(constraint_update, error_match):
    study = MagicMock(spec=Study)
    constraint = {
        "name": "g2p",
        "enabled": True,
        "type": "hourly",
        "operator": "equal",
        "node_1_left": "left_1",
        "node_2_left": "left_2",
        "node_right_area": {},
    }
    constraint.update(constraint_update)

    with pytest.raises(MEGenerationError, match=error_match):
        add_me_g2p_binding_constraints(study, [constraint])

    study.create_binding_constraint.assert_not_called()


def test_add_me_g2p_binding_constraints_wraps_unexpected_errors():
    study = MagicMock(spec=Study)
    study.create_binding_constraint.side_effect = Exception("backend failed")
    constraint = {
        "name": "g2p_euest",
        "enabled": True,
        "type": "hourly",
        "operator": "equal",
        "node_1_left": "left_1",
        "node_2_left": "left_2",
        "node_right_area": {},
    }

    with pytest.raises(MEGenerationError, match="g2p_euest"):
        add_me_g2p_binding_constraints(study, [constraint])
