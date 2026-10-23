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

import shutil

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from antares.craft import Month
from antares.datamanager.core.settings import GenerationMode, Settings
from antares.datamanager.exceptions.exceptions import FlowbasedGenerationError
from antares.datamanager.generator.flowbased_results import publish_flowbased_results
from antares.datamanager.generator.generate_flowbased import EXPECTED_HOURS, FlowbasedAllocation, FlowbasedFileReader
from antares.datamanager.generator.generate_study_process import generate_study
from antares.datamanager.models.study_data_json_model import StudyData


@pytest.fixture
def allocation():
    return FlowbasedAllocation(
        pd.DataFrame({0: ["winter1", "summer1"] * 4380, 1: ["winter2", "summer2"] * 4380}),
        pd.DataFrame({0: [1, 3] * 4380, 1: [2, 4] * 4380}),
    )


@pytest.fixture
def export_root(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_TRAJECTORY_FILE_PATH", "trajectories")
    monkeypatch.setenv("PEGASE_FLOWBASED_DIRECTORY", "flowbased")
    monkeypatch.setenv("PEGASE_FLOWBASED_RECALCULATE", "results")
    return tmp_path / "trajectories" / "flowbased" / "results"


def publish(allocation, name="study"):
    return publish_flowbased_results(allocation, name)


@pytest.mark.parametrize("trajectory", ["relative/trajectories", "/absolute/trajectories"])
def test_output_setting_is_under_flowbased_directory(trajectory, export_root, monkeypatch, tmp_path):
    monkeypatch.setenv("PEGASE_TRAJECTORY_FILE_PATH", trajectory)
    base = Path(trajectory) if Path(trajectory).is_absolute() else tmp_path / trajectory
    assert Settings().flowbased_recalculate_output_directory == base / "flowbased" / "results"


@pytest.mark.parametrize("configured", [None, "", "   "])
def test_export_requires_nonempty_configuration(configured, export_root, monkeypatch, allocation):
    if configured is None:
        monkeypatch.delenv("PEGASE_FLOWBASED_RECALCULATE", raising=False)
    else:
        monkeypatch.setenv("PEGASE_FLOWBASED_RECALCULATE", configured)
    with pytest.raises(FlowbasedGenerationError, match="configuration"):
        with publish(allocation):
            pytest.fail("Invalid configuration must not publish")


def test_export_contains_only_ts_matrix(export_root, allocation):
    with publish(allocation, "My study") as destination:
        assert destination == export_root / "My study"
    pd.testing.assert_frame_equal(FlowbasedFileReader.read_ts_file(destination / "ts.txt"), allocation.id_day_types)
    assert sorted(p.name for p in destination.iterdir()) == ["ts.txt"]
    assert sorted(p.name for p in export_root.iterdir()) == ["My study"]


def test_replacement_and_other_studies_are_isolated(export_root, allocation):
    with publish(allocation, "other"):
        pass
    with publish(allocation):
        pass
    for filename in ("clusters.tsv", "id_day_types.tsv", "metadata.json", "flowbased.md"):
        (export_root / "study" / filename).write_text("previous export")
    replacement = FlowbasedAllocation(allocation.clusters, allocation.id_day_types * 10)
    with publish(replacement):
        pass
    actual = FlowbasedFileReader.read_ts_file(export_root / "study" / "ts.txt")
    assert actual.iloc[0].tolist() == [10, 20]
    other = FlowbasedFileReader.read_ts_file(export_root / "other" / "ts.txt")
    assert other.iloc[0].tolist() == [1, 2]
    assert sorted(p.name for p in export_root.iterdir()) == ["other", "study"]
    assert sorted(p.name for p in (export_root / "study").iterdir()) == ["ts.txt"]


def test_ts_export_matches_reference_format(export_root, allocation):
    original_ids = allocation.id_day_types.copy()
    with publish(allocation) as destination:
        ts_path = destination / "ts.txt"
        lines = ts_path.read_text().splitlines()
        assert lines[0] == '"Date" "1" "2"'
        assert len(lines) == EXPECTED_HOURS + 1
        assert lines[1] == "1 1 2"
        assert lines[2] == "2 3 4"
        assert lines[-1] == "8760 3 4"
        actual = pd.read_csv(ts_path, sep=" ")
        assert actual.shape == (EXPECTED_HOURS, 3)
        assert actual["Date"].tolist() == list(range(1, EXPECTED_HOURS + 1))
        pd.testing.assert_frame_equal(FlowbasedFileReader.read_ts_file(ts_path), original_ids)
    pd.testing.assert_frame_equal(allocation.id_day_types, original_ids)


def test_ts_hours_are_independent_of_allocation_index(export_root, allocation):
    shifted = FlowbasedAllocation(
        allocation.clusters.set_axis(range(24, EXPECTED_HOURS + 24)),
        allocation.id_day_types.set_axis(range(24, EXPECTED_HOURS + 24)),
    )
    with publish(shifted) as destination:
        actual = pd.read_csv(destination / "ts.txt", sep=" ")
    assert actual["Date"].tolist() == list(range(1, EXPECTED_HOURS + 1))
    pd.testing.assert_frame_equal(
        actual.drop(columns="Date").set_axis(allocation.id_day_types.columns, axis=1),
        allocation.id_day_types,
    )


@pytest.mark.parametrize("previous", [False, True])
def test_failure_after_publication_restores_previous_results(export_root, allocation, previous):
    if previous:
        with publish(allocation):
            pass
    with pytest.raises(RuntimeError, match="Upload failed"):
        with publish(FlowbasedAllocation(allocation.clusters, allocation.id_day_types * 10)):
            raise RuntimeError("Upload failed")
    if previous:
        actual = FlowbasedFileReader.read_ts_file(export_root / "study" / "ts.txt")
        assert actual.iloc[0].tolist() == [1, 2]
    else:
        assert not (export_root / "study").exists()
    assert not list(export_root.glob(".*"))


def test_write_failure_preserves_old_results(export_root, allocation):
    with publish(allocation):
        pass
    with (
        patch.object(
            pd.DataFrame,
            "to_csv",
            side_effect=PermissionError("read only"),
        ),
        pytest.raises(FlowbasedGenerationError, match="read only"),
    ):
        with publish(allocation):
            pytest.fail("Must not publish an incomplete export")
    pd.testing.assert_frame_equal(
        FlowbasedFileReader.read_ts_file(export_root / "study" / "ts.txt"), allocation.id_day_types
    )
    assert not list(export_root.glob(".*"))


def test_concurrent_publication_is_rejected(export_root, allocation):
    with publish(allocation):
        with pytest.raises(FlowbasedGenerationError, match="already being published"):
            with publish(allocation):
                pytest.fail("Concurrent publication must not succeed")
        assert (export_root / ".study.lock").is_dir()
    with publish(allocation):
        pass


@pytest.mark.parametrize("name", ["", ".", "..", "../escape", "/absolute", "nested/study", "nested\\study"])
def test_invalid_study_name_is_rejected(export_root, allocation, name):
    with pytest.raises(FlowbasedGenerationError, match="Invalid study name"):
        with publish(allocation, name):
            pytest.fail("Invalid study name must not publish")
    assert not export_root.exists()


def test_misaligned_matrices_are_rejected(export_root, allocation):
    bad = FlowbasedAllocation(allocation.clusters.rename(columns={0: "other"}), allocation.id_day_types)
    with pytest.raises(FlowbasedGenerationError, match="aligned"):
        with publish(bad):
            pytest.fail("Misaligned matrices must not publish")


@pytest.mark.parametrize("shape", [(8784, 2), (8760, 0)])
def test_invalid_dimensions_are_rejected(export_root, shape):
    matrix = pd.DataFrame(0, index=range(shape[0]), columns=range(shape[1]))
    with pytest.raises(FlowbasedGenerationError, match="8760 rows"):
        with publish(FlowbasedAllocation(matrix, matrix)):
            pytest.fail("Invalid dimensions must not publish")


@pytest.mark.parametrize("mode", [GenerationMode.API, GenerationMode.LOCAL])
@pytest.mark.parametrize("recalculate", [False, True])
@pytest.mark.parametrize("upload_failure", [False, True])
def test_generate_study_publishes_only_recalculated_results(
    export_root, allocation, monkeypatch, tmp_path, mode, recalculate, upload_failure
):
    monkeypatch.setenv("GENERATION_MODE", mode.value)
    if not recalculate:
        monkeypatch.delenv("PEGASE_FLOWBASED_RECALCULATE", raising=False)
    study_data = StudyData(name="study", flowbased={"recalculate_ts": recalculate, "ts_path": "model", "type_days": []})
    study = MagicMock()
    local_study = tmp_path / "temporary-study"
    local_study.mkdir()
    study.path = str(local_study)
    study.get_settings.return_value.general_parameters.first_month_in_year = Month.JULY
    factory = MagicMock()
    factory.create_study.return_value = study
    if recalculate and upload_failure:
        with publish(FlowbasedAllocation(allocation.clusters, allocation.id_day_types * 10)):
            pass

    def package_and_clean(name):
        if recalculate:
            assert (export_root / name / "ts.txt").exists()
        shutil.rmtree(local_study)
        if upload_failure:
            raise RuntimeError("Upload failed")

    module = "antares.datamanager.generator.generate_study_process"
    with ExitStack() as stack:
        for function in (
            "build_study_settings",
            "add_areas_to_study",
            "add_links_to_study",
            "create_flowbased_areas_and_links",
            "generate_scenario_builder",
        ):
            stack.enter_context(patch(f"{module}.{function}"))
        stack.enter_context(patch(f"{module}.read_study_data_from_json", return_value=study_data))
        stack.enter_context(
            patch(f"{module}.generate_flowbased_binding_constraints", return_value=allocation if recalculate else None)
        )
        package = stack.enter_context(patch(f"{module}._package_and_upload_local_study", side_effect=package_and_clean))
        if upload_failure and mode == GenerationMode.LOCAL:
            with pytest.raises(RuntimeError, match="Upload failed"):
                generate_study("request-id", factory)
            if recalculate:
                restored = FlowbasedFileReader.read_ts_file(export_root / "study" / "ts.txt")
                assert restored.iloc[0].tolist() == [10, 20]
            assert not list(export_root.glob(".*"))
            return
        response = generate_study("request-id", factory)
    assert package.call_count == int(mode == GenerationMode.LOCAL)
    if mode == GenerationMode.LOCAL:
        assert not local_study.exists()
    if recalculate:
        assert response["flowbased_results_path"] == str(export_root / "study")
        assert (export_root / "study" / "ts.txt").exists()
    else:
        assert "flowbased_results_path" not in response
        assert not export_root.exists()


def test_failed_replacement_restores_previous_directory(export_root, allocation):
    with publish(allocation):
        pass
    rename = Path.rename

    def fail_new_directory(source, target):
        if source.name == "new":
            raise PermissionError("Cannot publish")
        return rename(source, target)

    with (
        patch.object(Path, "rename", fail_new_directory),
        pytest.raises(FlowbasedGenerationError, match="Cannot publish"),
    ):
        with publish(FlowbasedAllocation(allocation.clusters, allocation.id_day_types * 10)):
            pytest.fail("Replacement must fail")
    restored = FlowbasedFileReader.read_ts_file(export_root / "study" / "ts.txt")
    assert restored.iloc[0].tolist() == [1, 2]
    assert not list(export_root.glob(".*"))


def test_restore_failure_keeps_backup_for_recovery(export_root, allocation, caplog):
    with publish(allocation):
        pass
    rename = Path.rename

    def fail_restore(source, target):
        if source.name == "previous":
            raise PermissionError("Cannot restore")
        return rename(source, target)

    with (
        patch.object(Path, "rename", fail_restore),
        pytest.raises(FlowbasedGenerationError, match="Cannot restore"),
    ):
        with publish(allocation):
            raise RuntimeError("Upload failed")
    backups = list(export_root.glob(".study-*/previous"))
    assert len(backups) == 1
    assert (backups[0] / "ts.txt").exists()
    assert str(backups[0]) in caplog.text
