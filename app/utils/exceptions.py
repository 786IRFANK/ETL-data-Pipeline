class AppError(Exception):
    status_code = 500
    message = "Application error"

    def __init__(self, message: str | None = None):
        if message:
            self.message = message


class ValidationError(AppError):
    status_code = 400


class NotFoundError(AppError):
    status_code = 404


class ExternalServiceError(AppError):
    status_code = 502


class DatabaseError(AppError):
    status_code = 500
