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

import pandas as pd

from antares.datamanager.exceptions.exceptions import PmmlModelError
from antares.datamanager.utils.random_forest_reader import load_forest_model, predict_cluster, predict_clusters_batch

# Sample PMML file

FEATURE_FIELDS = ["x1", "x2", "x3"]


def _single_split_tree_segment(segment_id: int, field: str) -> str:
    """A tree that votes "cat_a" if `field` <= 0.5, else "cat_b"."""
    return f"""
   <Segment id="{segment_id}">
    <True/>
    <TreeModel modelName="sample_forest" functionName="classification" algorithmName="randomForest" splitCharacteristic="binarySplit">
     <MiningSchema>
      <MiningField name="category" usageType="predicted" invalidValueTreatment="asIs"/>
      <MiningField name="{field}" usageType="active" invalidValueTreatment="asIs"/>
     </MiningSchema>
     <Node id="1">
      <True/>
      <Node id="2" score="cat_a">
       <SimplePredicate field="{field}" operator="lessOrEqual" value="0.5"/>
      </Node>
      <Node id="3" score="cat_b">
       <SimplePredicate field="{field}" operator="greaterThan" value="0.5"/>
      </Node>
     </Node>
    </TreeModel>
   </Segment>"""


# One tree per feature (majority vote of x1/x2/x3 <= 0.5 decides "cat_a" vs "cat_b").
_TREE_SEGMENTS = "".join(_single_split_tree_segment(i, field) for i, field in enumerate(FEATURE_FIELDS, start=1))
_ACTIVE_FIELDS = "\n   ".join(
    f'<MiningField name="{field}" usageType="active" invalidValueTreatment="returnInvalid"/>'
    for field in FEATURE_FIELDS
)
_DATA_FIELDS = "\n  ".join(
    f'<DataField name="{field}" optype="continuous" dataType="double"/>' for field in FEATURE_FIELDS
)

SAMPLE_PMML = f"""<?xml version="1.0"?>
<PMML version="4.4.1" xmlns="http://www.dmg.org/PMML-4_4">
 <Header copyright="Copyright (c) 1970 Placeholder" description="Fabricated sample model for unit tests">
  <Application name="Fake PMML Generator" version="0.0.0"/>
  <Timestamp>1970-01-01 00:00:00</Timestamp>
 </Header>
 <DataDictionary numberOfFields="{len(FEATURE_FIELDS) + 1}">
  <DataField name="category" optype="categorical" dataType="string">
   <Value value="cat_a"/>
   <Value value="cat_b"/>
  </DataField>
  {_DATA_FIELDS}
 </DataDictionary>
 <MiningModel modelName="sample_forest" algorithmName="randomForest" functionName="classification">
  <MiningSchema>
   <MiningField name="category" usageType="predicted" invalidValueTreatment="returnInvalid"/>
   {_ACTIVE_FIELDS}
  </MiningSchema>
  <Segmentation multipleModelMethod="majorityVote">{_TREE_SEGMENTS}
  </Segmentation>
 </MiningModel>
</PMML>
"""


@pytest.fixture
def sample_pmml_path(tmp_path: Path) -> Path:
    pmml_path = tmp_path / "sample_forest.pmml"
    pmml_path.write_text(SAMPLE_PMML)
    return pmml_path


def test_should_load_forest_model_from_pmml_file(sample_pmml_path):
    model = load_forest_model(sample_pmml_path)

    assert list(model.classes_) == ["cat_a", "cat_b"]


def test_should_predict_cluster_from_majority_vote(sample_pmml_path):
    model = load_forest_model(sample_pmml_path)

    low = {"x1": 0.1, "x2": 0.1, "x3": 0.1}
    high = {"x1": 0.9, "x2": 0.9, "x3": 0.9}
    mixed = {"x1": 0.1, "x2": 0.9, "x3": 0.9}

    assert predict_cluster(model, low) == "cat_a"
    assert predict_cluster(model, high) == "cat_b"
    assert predict_cluster(model, mixed) == "cat_b"


def test_should_predict_clusters_batch_for_multiple_rows(sample_pmml_path):
    model = load_forest_model(sample_pmml_path)

    features_df = pd.DataFrame(
        [
            {"x1": 0.1, "x2": 0.1, "x3": 0.1},
            {"x1": 0.9, "x2": 0.9, "x3": 0.9},
            {"x1": 0.1, "x2": 0.9, "x3": 0.9},
        ]
    )

    assert predict_clusters_batch(model, features_df) == ["cat_a", "cat_b", "cat_b"]


def test_should_raise_pmml_model_error_when_batch_prediction_fails(sample_pmml_path):
    model = load_forest_model(sample_pmml_path)

    with pytest.raises(PmmlModelError):
        predict_clusters_batch(model, pd.DataFrame([{"x1": 0.1}]))


def test_should_raise_pmml_model_error_when_file_is_missing():
    with pytest.raises(PmmlModelError):
        load_forest_model(Path("/nonexistent/random_forest.pmml"))


def test_should_raise_pmml_model_error_when_file_is_malformed(tmp_path):
    malformed_pmml = tmp_path / "malformed.pmml"
    malformed_pmml.write_text("<PMML><MiningModel>")

    with pytest.raises(PmmlModelError):
        load_forest_model(malformed_pmml)
