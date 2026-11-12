"""Service-level errors. The API layer maps each class to one HTTP status."""

from typing import Any


class ServiceError(Exception):
    status_code = 500

    def __init__(self, code: str, message: str, **details: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


class NotFound(ServiceError):
    status_code = 404


class Conflict(ServiceError):
    status_code = 409


class Unprocessable(ServiceError):
    status_code = 422
