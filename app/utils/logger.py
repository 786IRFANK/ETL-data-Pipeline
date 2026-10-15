import logging
import os
from typing import Any, Dict

import structlog

from app.config.settings import settings


def _get_log_level() -> int:
    env = (settings.ENVIRONMENT or "").lower()
    if env in {"prod", "production", "staging"}:
        return logging.INFO
    return logging.DEBUG if getattr(settings, "DEBUG", False) else logging.INFO


def _configure_standard_logging() -> None:
    log_level = _get_log_level()

    root = logging.getLogger()
    if getattr(root, "_axonal_logging_configured", False):
        return

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))  # structlog will format

    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

    root._axonal_logging_configured = True  # type: ignore[attr-defined]


def _configure_structlog() -> None:
    _configure_standard_logging()

    if getattr(structlog, "_axonal_configured", False):
        return

    processors = [
        structlog.contextvars.merge_contextvars,  # support contextvars (request ids, etc.)
        structlog.processors.TimeStamper(fmt="iso", key="timestamp"),
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,  # include exception stack traces
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),  # final renderer -> JSON
    ]

    structlog.configure(
        processors=processors,
        context_class=dict,  # simple dict for context
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    setattr(structlog, "_axonal_configured", True)


def get_logger(name: str | None = None, **initial_context: Dict[str, Any]):
    
    _configure_structlog()

    logger = structlog.get_logger(name or __name__)

    # Bind environment and any initial context (service_name, component, etc.)
    base_context: Dict[str, Any] = {
        "environment": settings.ENVIRONMENT,
        "app": getattr(settings, "APP_NAME", "axonal-data-platform"),
    }
    if initial_context:
        base_context.update(initial_context)

    return logger.bind(**base_context)
