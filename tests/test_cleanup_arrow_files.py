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

from antares.datamanager.generator.generate_study_process import generate_study
from antares.datamanager.utils.arrow_cleanup_utils import ArrowCleanupUtils


def test_cleanup_arrow_files_basic(tmp_path):
    # Setup: Create some dummy arrow files
    file1 = tmp_path / "file1.arrow"
    file2 = tmp_path / "file2.arrow"
    file1.touch()
    file2.touch()

    # Non-arrow file and an arrow file NOT in the set
    file3 = tmp_path / "file3.txt"
    file3.touch()
    file4 = tmp_path / "file4.arrow"
    file4.touch()

    used_files = {file1, file2, file3}  # file3 is .txt, should be ignored by logic

    # Action
    ArrowCleanupUtils.cleanup_arrow_files(used_files)

    # Assert
    assert not file1.exists()
    assert not file2.exists()
    assert file3.exists()  # Not an .arrow file should be ignored
    assert file4.exists()  # Not in the used_files set


def test_cleanup_arrow_files_non_existent(tmp_path):
    # Setup: A file path that doesn't exist
    file_path = tmp_path / "non_existent.arrow"
    used_files = {file_path}

    # Action & Assert: Should not raise any error
    ArrowCleanupUtils.cleanup_arrow_files(used_files)


def test_cleanup_arrow_files_error_handling(tmp_path):
    # Setup: Create a file and make it non-deletable (or just mock unlink to raise)
    file_path = tmp_path / "protected.arrow"
    file_path.touch()

    with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
        # Action & Assert: Should not raise error due to try-except in cleanup_arrow_files
        ArrowCleanupUtils.cleanup_arrow_files({file_path})

    assert file_path.exists()


@patch("antares.datamanager.generator.generate_study_process.read_study_data_from_json")
@patch("antares.datamanager.utils.arrow_cleanup_utils.ArrowCleanupUtils.cleanup_arrow_files")
def test_generate_study_calls_cleanup(mock_cleanup, mock_read_json):
    # Setup
    mock_factory = MagicMock()
    mock_study = MagicMock()
    mock_study.path = Path("/mock/study")
    mock_factory.create_study.return_value = mock_study

    from antares.datamanager.models.study_data_json_model import StudyData

    mock_read_json.return_value = StudyData(name="test_study")

    # Action
    generate_study("study_id", mock_factory)

    # Assert
    mock_cleanup.assert_called_once()
    used_files = mock_cleanup.call_args[0][0]
    assert isinstance(used_files, set)


@patch("antares.datamanager.generator.generate_study_process.read_study_data_from_json")
@patch("antares.datamanager.generator.generate_study_process.add_areas_to_study")
@patch("antares.datamanager.utils.arrow_cleanup_utils.ArrowCleanupUtils.cleanup_arrow_files")
def test_generate_study_cleanup_on_failure(mock_cleanup, mock_add_areas, mock_read_json):
    # Setup
    mock_factory = MagicMock()
    mock_study = MagicMock()
    mock_study.path = Path("/mock/study")
    mock_factory.create_study.return_value = mock_study

    from antares.datamanager.models.study_data_json_model import StudyData

    mock_read_json.return_value = StudyData(name="test_study")

    # Simulate failure during generation
    import pytest

    mock_add_areas.side_effect = Exception("Generation failed")

    # Action
    with pytest.raises(Exception, match="Generation failed"):
        generate_study("study_id", mock_factory)

    # Assert: Cleanup should have been called even on failure
    mock_cleanup.assert_called_once()
