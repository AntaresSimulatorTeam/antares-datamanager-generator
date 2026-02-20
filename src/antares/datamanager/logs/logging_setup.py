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

# File: src/antares/datamanager/logging_setup.py
import json
import logging
import os

from datetime import datetime

from typing_extensions import override

ECS_VERSION = "1.6.0"


class ECSJSONFormatter(logging.Formatter):
    @override
    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        payload = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "ecs.version": ECS_VERSION,
            "log.level": record.levelname,
            "log.logger": record.name,
            "application": "pegase-generator",
            "message": msg,
            "process.pid": os.getpid(),
            "thread.name": getattr(record, "threadName", None),
        }

        # Extra fields passés via `extra={...}`
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k
            not in (
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
            )
        }
        if extras:
            payload.update(extras)

        # Si une exception est attachée, ajouter les champs d'erreur ECS
        if record.exc_info:
            try:
                # stack trace complète
                payload["error.stack_trace"] = self.formatException(record.exc_info)
                exc_type, exc_value, _ = record.exc_info
                payload["error.type"] = getattr(exc_type, "__name__", str(exc_type))
                payload["error.message"] = str(exc_value)
            except Exception:
                # Ne pas casser le logging si le formatage échoue
                payload.setdefault("error.message", "Could not format exception")

        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_ecs_logger(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(ECSJSONFormatter())
    root.setLevel(level)
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
