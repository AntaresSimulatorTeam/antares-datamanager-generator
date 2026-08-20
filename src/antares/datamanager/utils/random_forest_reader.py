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
import xml.etree.ElementTree as ET

from pathlib import Path

import pandas as pd

from sklearn_pmml_model.ensemble import PMMLForestClassifier

from antares.datamanager.exceptions.exceptions import PmmlModelError
from antares.datamanager.logs.logging_setup import get_logger

logger = get_logger(__name__)


def load_forest_model(pmml_path: Path) -> PMMLForestClassifier:
    """Load a Random Forest classifier from R pmml package

    Args:
        pmml_path: Path to the .pmml file

    Returns:
        A classifier ready for prediciton

    Raises:
        PmmlModelError: If the file is missing, or incorrect
    """
    try:
        model = PMMLForestClassifier(pmml=str(pmml_path))
    except (OSError, ET.ParseError, KeyError, ValueError) as exc:
        raise PmmlModelError(f"Could not load PMML model from {pmml_path}: {exc}") from exc

    logger.info("Loaded PMML random forest model", extra={"pmml_path": str(pmml_path)})
    return model


def predict_cluster(model: PMMLForestClassifier, features: dict[str, float]) -> str:
    """Predict the weather cluster label for a single set of features.

    Args:
        model: A classifier loaded with the `load_forest_model` method
        features: Mapping of PMML field name to its value.

    Returns:
        The predicted cluster label (example: "summer2")

    Raises:
        PmmlModelError: If prediction fails
    """
    try:
        prediction = model.predict(pd.DataFrame([features]))
    except Exception as exc:
        raise PmmlModelError(f"Could not predict cluster: {exc}") from exc

    return str(prediction[0])


def predict_clusters_batch(model: PMMLForestClassifier, features_df: pd.DataFrame) -> list[str]:
    """Predict the weather cluster label for many rows of features at once.

    Vectorized version of `predict_cluster`, used when doing a full hourly
    instead of a single row at a time.

    Args:
        model: A classifier loaded with the `load_forest_model` method
        features_df: One row per prediction, one column per PMML field name.

    Returns:
        The predicted cluster labels, in the same row order as `features_df`.

    Raises:
        PmmlModelError: If prediction fails
    """
    try:
        predictions = model.predict(features_df)
    except Exception as exc:
        raise PmmlModelError(f"Could not predict clusters: {exc}") from exc

    return [str(prediction) for prediction in predictions]
