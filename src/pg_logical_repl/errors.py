"""Stable domain errors; never expose raw driver/child exceptions."""


class ConnectorError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        exit_code: int = 6,
        retryable: bool = False,
        recovery_hint: str = "",
        sqlstate: str | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.retryable = retryable
        self.recovery_hint = recovery_hint
        self.sqlstate = sqlstate

    def as_dict(self):
        return dict(
            code=self.code,
            message=str(self),
            retryable=self.retryable,
            recovery_hint=self.recovery_hint,
            sqlstate=self.sqlstate,
        )


class ConfigurationError(ConnectorError):
    def __init__(self, message):
        super().__init__("CONFIGURATION_ERROR", message, exit_code=2)


class PreconditionError(ConnectorError):
    def __init__(self, code, message, **kwargs):
        super().__init__(code, message, exit_code=3, **kwargs)


class OwnershipError(ConnectorError):
    def __init__(self, message):
        super().__init__("OWNERSHIP_ERROR", message, exit_code=8)
