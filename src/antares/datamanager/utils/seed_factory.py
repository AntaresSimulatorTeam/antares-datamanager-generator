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

import hashlib

from typing import Union


class SeedFactory:
    """
    Utility to generate deterministic integer seeds from arbitrary inputs.
    """

    DEFAULT_MODULO = 2**32  # Compatible with most RNGs

    @staticmethod
    def _normalize(value: Union[str, int]) -> str:
        """Convert input to a stable string representation."""
        return str(value)

    @classmethod
    def from_string(cls, seed_str: str, modulo: int = 0) -> int:
        """Convert a string into a deterministic integer seed."""
        modulo = modulo or cls.DEFAULT_MODULO

        hash_bytes = hashlib.sha256(seed_str.encode("utf-8")).digest()
        seed_int = int.from_bytes(hash_bytes, "big")

        return seed_int % modulo

    @classmethod
    def from_components(cls, *components: Union[str, int], modulo: int = 0) -> int:
        """
        Build a seed from multiple components (recommended entry point).

        Example:
            SeedFactory.from_components(seed_tsgen_link, link_name)
        """
        normalized = [cls._normalize(c) for c in components]
        seed_str = "-".join(normalized)
        return cls.from_string(seed_str, modulo)

    @classmethod
    def for_timeseries(cls, seed_tsgen_link: int, link_name: str) -> int:
        """
        Build a seed from a general seed and a link name.
        The general seed is an integer, and the link name is a string.
        A deterministic integer seed is returned for MersenneTwisterRNG.
        """
        return cls.from_components(seed_tsgen_link, link_name)
