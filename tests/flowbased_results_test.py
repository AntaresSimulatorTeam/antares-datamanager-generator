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

import json
import shutil

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from antares.craft import Month
from antares.datamanager.core.settings import GenerationMode, Settings
from antares.datamanager.exceptions.exceptions import FlowbasedGenerationError
from antares.datamanager.generator.flowbased_results import publish_flowbased_results
from antares.datamanager.generator.generate_flowbased import EXPECTED_HOURS, FlowbasedAllocation
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
    root = tmp_path / "results"
    monkeypatch.setenv("PEGASE_FLOWBASED_RECALCULATE_OUTPUT_DIRECTORY", str(root))
    return root


def publish(allocation, name="study"):
    return publish_flowbased_results(
        allocation, name, "request-id", {"ts_path": "flowbased/model", "type_days": []}, Month.JULY
    )


@pytest.mark.parametrize("configured", ["/absolute/results", "relative/results"])
def test_output_setting_resolves_paths(configured, monkeypatch, tmp_path):
    monkeypatch.setenv("PEGASE_FLOWBASED_RECALCULATE_OUTPUT_DIRECTORY", configured)
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    expected = Path(configured) if Path(configured).is_absolute() else tmp_path / configured
    assert Settings().flowbased_recalculate_output_directory == expected


@pytest.mark.parametrize("configured", [None, "", "   "])
def test_export_requires_nonempty_configuration(configured, monkeypatch, allocation):
    if configured is None:
        monkeypatch.delenv("PEGASE_FLOWBASED_RECALCULATE_OUTPUT_DIRECTORY", raising=False)
    else:
        monkeypatch.setenv("PEGASE_FLOWBASED_RECALCULATE_OUTPUT_DIRECTORY", configured)
    with pytest.raises(FlowbasedGenerationError, match="configuration"):
        with publish(allocation):
            pytest.fail("Invalid configuration must not publish")


def test_export_roundtrip_and_metadata(export_root, allocation):
    with publish(allocation, "My study") as destination:
        assert destination == export_root / "My study"
    for filename, expected in (("clusters.tsv", allocation.clusters), ("id_day_types.tsv", allocation.id_day_types)):
        actual = pd.read_csv(destination / filename, sep="\t")
        assert actual.shape == (EXPECTED_HOURS, 2)
        expected = expected.set_axis(["scenario_1", "scenario_2"], axis=1)
        pd.testing.assert_frame_equal(actual, expected)
    metadata = json.loads((destination / "metadata.json").read_text())
    assert metadata["study_name"] == "My study"
    assert metadata["study_id"] == "request-id"
    assert metadata["first_month"] == Month.JULY.value
    assert metadata["antares_series_indices"] == [1, 2]
    assert sorted(p.name for p in export_root.iterdir()) == ["My study"]


def test_replacement_and_other_studies_are_isolated(export_root, allocation):
    with publish(allocation, "other"):
        pass
    with publish(allocation):
        pass
    replacement = FlowbasedAllocation(allocation.clusters, allocation.id_day_types * 10)
    with publish(replacement):
        pass
    actual = pd.read_csv(export_root / "study" / "id_day_types.tsv", sep="\t")
    assert actual.iloc[0].tolist() == [10, 20]
    other = pd.read_csv(export_root / "other" / "id_day_types.tsv", sep="\t")
    assert other.iloc[0].tolist() == [1, 2]
    assert sorted(p.name for p in export_root.iterdir()) == ["other", "study"]


@pytest.mark.parametrize("previous", [False, True])
def test_failure_after_publication_restores_previous_results(export_root, allocation, previous):
    if previous:
        with publish(allocation):
            pass
    with pytest.raises(RuntimeError, match="Upload failed"):
        with publish(FlowbasedAllocation(allocation.clusters, allocation.id_day_types * 10)):
            raise RuntimeError("Upload failed")
    if previous:
        actual = pd.read_csv(export_root / "study" / "id_day_types.tsv", sep="\t")
        assert actual.iloc[0].tolist() == [1, 2]
    else:
        assert not (export_root / "study").exists()
    assert not list(export_root.glob(".*"))


@pytest.mark.parametrize("fail_at", [1, 2])
def test_write_failure_preserves_old_results(export_root, allocation, fail_at):
    with publish(allocation):
        pass
    to_csv = pd.DataFrame.to_csv
    calls = 0

    def failing_write(frame, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == fail_at:
            raise PermissionError("read only")
        return to_csv(frame, *args, **kwargs)

    with (
        patch.object(pd.DataFrame, "to_csv", failing_write),
        pytest.raises(FlowbasedGenerationError, match="read only"),
    ):
        with publish(allocation):
            pytest.fail("Must not publish an incomplete export")
    assert (export_root / "study" / "metadata.json").exists()
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
    export_root, allocation, monkeypatch, mode, recalculate, upload_failure
):
    monkeypatch.setenv("GENERATION_MODE", mode.value)
    if not recalculate:
        monkeypatch.delenv("PEGASE_FLOWBASED_RECALCULATE_OUTPUT_DIRECTORY")
    study_data = StudyData(name="study", flowbased={"recalculate_ts": recalculate, "ts_path": "model", "type_days": []})
    study = MagicMock()
    local_study = export_root.parent / "temporary-study"
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
            assert (export_root / name / "clusters.tsv").exists()
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
                restored = pd.read_csv(export_root / "study" / "id_day_types.tsv", sep="\t")
                assert restored.iloc[0].tolist() == [10, 20]
            assert not list(export_root.glob(".*"))
            return
        response = generate_study("request-id", factory)
    assert package.call_count == int(mode == GenerationMode.LOCAL)
    if mode == GenerationMode.LOCAL:
        assert not local_study.exists()
    if recalculate:
        assert response["flowbased_results_path"] == str(export_root / "study")
        assert (export_root / "study" / "clusters.tsv").exists()
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
    restored = pd.read_csv(export_root / "study" / "id_day_types.tsv", sep="\t")
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
    assert (backups[0] / "clusters.tsv").exists()
    assert str(backups[0]) in caplog.text
