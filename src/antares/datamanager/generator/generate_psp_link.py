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

"""
PSP (Pumped Storage Plant) Virtual Area Link Generation.

This module handles the automatic creation of links between real areas and their virtual
PSP areas with capacities loaded from PSP maxpower data files.
"""

import logging

from pathlib import Path
from typing import Set

import pandas as pd

from antares.craft import LinkPropertiesUpdate
from antares.craft.model.study import Study
from antares.datamanager.exceptions.exceptions import APIGenerationError, LinkGenerationError
from antares.datamanager.generator.generate_link_matrices import (
    generate_constant_link_capacity_df,
    generate_link_parameters_df,
)

logger = logging.getLogger(__name__)


def get_maxpower_data(area_name: str, used_files: Set[Path]) -> tuple[float, float]:
    """
    Load maxpower data from the PSP maxpower file for a given area.

    Args:
        area_name: Area name (e.g., 'AT', 'FR')
        used_files: Set to track used files

    Returns:
        tuple[float, float]: (max_pumping, max_generating) values
    """
    expected_prefix = f"{area_name}_psp_maxpower.".lower()
    maxpower_file = next(
        (
            path
            for path in used_files
            if path.name.lower().startswith(expected_prefix) and path.suffix.lower() == ".arrow"
        ),
        None,
    )

    if maxpower_file is None or not maxpower_file.exists():
        logger.warning(
            f"PSP maxpower file not found in used files for area {area_name}, using zero capacities for PSP link"
        )
        return 0.0, 0.0

    try:
        used_files.add(maxpower_file)
        df = pd.read_feather(maxpower_file)

        area_lower = area_name.lower()
        pumping_col = f"{area_lower}_pumping"
        generating_col = f"{area_lower}_generating"

        # Find columns case-insensitively
        columns_lower = {str(col).lower(): col for col in df.columns}

        max_pumping = 0.0
        max_generating = 0.0

        if pumping_col in columns_lower:
            max_pumping = float(df[columns_lower[pumping_col]].max())

        if generating_col in columns_lower:
            max_generating = float(df[columns_lower[generating_col]].max())

        logger.info(f"Loaded PSP maxpower data for {area_name}: pumping={max_pumping}, generating={max_generating}")
        return max_pumping, max_generating

    except Exception as e:
        logger.error(f"Error loading PSP maxpower data for {area_name}: {e}")
        return 0.0, 0.0


def create_psp_link(study: Study, real_area_name: str, virtual_area_name: str, used_files: Set[Path]) -> None:
    """
    Create a link between a real area and its virtual PSP area with capacities from MaxPower data.

    Steps:
    a) Create link: real_area <-> virtual_area
    b) Create direct capacity matrix from maxpower pumping column
    c) Create indirect capacity matrix from maxpower generating column
    d) Set hurdle costs to 0.01 for both directions

    Args:
        study: Antares Study object
        real_area_name: Name of the real area
        virtual_area_name: Name of the virtual PSP area
        used_files: Set to track used files
    """
    area_from = real_area_name.lower()
    area_to = virtual_area_name.lower()

    max_pumping, max_generating = get_maxpower_data(real_area_name, used_files)

    try:
        # a) Create link
        link = study.create_link(area_from=area_from, area_to=area_to)
        logger.info(f"Created PSP link from {area_from} to {area_to}")

        # b) Create and set direct capacity matrix (pumping)
        df_capacity_direct = generate_constant_link_capacity_df(max_pumping)
        link.set_capacity_direct(df_capacity_direct)
        logger.info(f"Set direct capacity ({max_pumping} MW) for PSP link {area_from}->{area_to}")

        # c) Create and set indirect capacity matrix (generating)
        df_capacity_indirect = generate_constant_link_capacity_df(max_generating)
        link.set_capacity_indirect(df_capacity_indirect)
        logger.info(f"Set indirect capacity ({max_generating} MW) for PSP link {area_from}->{area_to}")

        # d) Set hurdle costs to 0.01
        df_parameters = generate_link_parameters_df(0.01)
        link.update_properties(LinkPropertiesUpdate(hurdles_cost=True))
        link.set_parameters(df_parameters)
        logger.info(f"Set hurdle costs (0.01) for PSP link {area_from}->{area_to}")

    except APIGenerationError as e:
        raise LinkGenerationError(area_from, area_to, f"Failed to create PSP link from {area_from} to {area_to}") from e
