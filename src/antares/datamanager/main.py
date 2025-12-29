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
from typing import Annotated

import uvicorn
from antares.craft import APIconf

from fastapi import FastAPI, HTTPException, Depends

from antares.datamanager.APIGeneratorConfig.config import APIGeneratorConfig
from antares.datamanager.core.dependencies import get_study_factory
from antares.datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError, LinkGenerationError
from antares.datamanager.generator.study_adapters import APIStudyFactory, StudyFactory
from antares.datamanager.generator.generate_study_process import generate_study

app = FastAPI(
    title="datamanager-datamanager-generator", description="API to launch datamanager study generation", version="0.0.1"
)

@app.post("/generate_study/")
def create_study(
    study_id: str,
    factory: Annotated[StudyFactory, Depends(get_study_factory)]
) -> dict[str, str]:
    """
    Generates an antrares study
    The mode (API, LOCAL) is determined by GENERATION_MODE environment variable
    """
    try:
        return generate_study(study_id, factory)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (APIGenerationError, AreaGenerationError, LinkGenerationError) as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8094, reload=True)
