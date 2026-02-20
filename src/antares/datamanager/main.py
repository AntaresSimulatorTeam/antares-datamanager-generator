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

# python
import logging

from typing import Annotated

import uvicorn

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from antares.datamanager.core.dependencies import get_study_factory
from antares.datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError, LinkGenerationError
from antares.datamanager.generator.generate_study_process import generate_study
from antares.datamanager.generator.study_adapters import StudyFactory

# Logger basic configuration for ecs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("antares.datamanager")

app = FastAPI(
    title="datamanager-datamanager-generator", description="API to launch datamanager study generation", version="0.0.1"
)


@app.post("/generate_study/")
def create_study(study_id: str, factory: Annotated[StudyFactory, Depends(get_study_factory)]) -> dict[str, str]:
    try:
        return generate_study(study_id, factory)
    except FileNotFoundError as e:
        logger.exception("File not found while generating study", exc_info=True, extra={"study_id": study_id})
        raise HTTPException(status_code=404, detail=str(e))
    except (APIGenerationError, AreaGenerationError, LinkGenerationError) as e:
        logger.exception("Generation error", exc_info=True, extra={"study_id": study_id})
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        # Complete exception (stack trace) to appear clearly in logs
        logger.exception("Internal error while generating study", exc_info=True, extra={"study_id": study_id})
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")


# Global handler to log all exceptions no intercepted
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "Unhandled exception on request", exc_info=True, extra={"method": request.method, "path": str(request.url)}
    )
    return JSONResponse(status_code=500, content={"detail": f"Internal Error: {str(exc)}"})


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8094, reload=True)
