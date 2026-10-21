import logging

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, InterfaceError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from app.domain.exceptions import (
    IdempotencyKeyReusedError,
    PaymentNotFoundError,
    WebhookUrlNotAllowedError,
)

# SQLSTATE classes of a server that is unreachable, overloaded or shutting down
UNAVAILABLE_SQLSTATES = frozenset({"08", "53", "57"})

logger = logging.getLogger(__name__)


def _error(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


def _validation_error(loc: list[str], message: str, error_type: str) -> JSONResponse:
    # same shape as FastAPI's own 422, so clients and OpenAPI deal with a single format
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": [{"type": error_type, "loc": loc, "msg": message}]},
    )


async def validation_failed(_: Request, exc: Exception) -> JSONResponse:
    errors = exc.errors() if isinstance(exc, RequestValidationError) else []
    # the input is not echoed: it may be large, sensitive or not even JSON-encodable (NaN)
    detail = [{key: value for key, value in error.items() if key != "input"} for error in errors]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": jsonable_encoder(detail)},
    )


async def not_found(_: Request, exc: Exception) -> JSONResponse:
    return _error(status.HTTP_404_NOT_FOUND, str(exc))


async def idempotency_key_reused(_: Request, exc: Exception) -> JSONResponse:
    return _validation_error(["header", "Idempotency-Key"], str(exc), "idempotency_key_reused")


async def webhook_url_not_allowed(_: Request, exc: Exception) -> JSONResponse:
    return _validation_error(["body", "webhook_url"], str(exc), "webhook_url_not_allowed")


async def database_unavailable(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "database unavailable",
        extra={"path": request.url.path, "error": f"{type(exc).__name__}: {exc}"},
    )
    return _error(status.HTTP_503_SERVICE_UNAVAILABLE, "Service temporarily unavailable")


async def database_error(request: Request, exc: Exception) -> JSONResponse:
    sqlstate = str(getattr(getattr(exc, "orig", None), "sqlstate", None) or "")
    invalidated = isinstance(exc, DBAPIError) and exc.connection_invalidated
    if invalidated or isinstance(exc, InterfaceError) or sqlstate[:2] in UNAVAILABLE_SQLSTATES:
        return await database_unavailable(request, exc)
    logger.error("database error", extra={"path": request.url.path}, exc_info=exc)
    return _error(status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error")


async def unhandled_error(request: Request, exc: Exception) -> JSONResponse:
    # Starlette re-raises after this handler and uvicorn logs the traceback, so none here
    logger.error("unhandled error", extra={"path": request.url.path, "error": repr(exc)})
    return _error(status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error")


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, validation_failed)
    app.add_exception_handler(PaymentNotFoundError, not_found)
    app.add_exception_handler(IdempotencyKeyReusedError, idempotency_key_reused)
    app.add_exception_handler(WebhookUrlNotAllowedError, webhook_url_not_allowed)
    app.add_exception_handler(DBAPIError, database_error)
    # a pool wait timeout and asyncpg's bare OSError (refused, DNS, timeout) mean no database
    app.add_exception_handler(PoolTimeoutError, database_unavailable)
    app.add_exception_handler(OSError, database_unavailable)
    app.add_exception_handler(Exception, unhandled_error)
