"""JSON event logging; the service passes only redacted records."""

import logging
import os
import sys


def configure(level="INFO", path=None):
    logger = logging.getLogger("pg_logical_repl")
    for handler in logger.handlers:
        handler.close()
    logger.handlers = []
    logger.propagate = False
    logger.setLevel(level.upper())
    streams = [sys.stderr]
    if path:
        fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        os.fchmod(fd, 0o600)
        streams.append(os.fdopen(fd, "a"))
    for stream in streams:
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)


def close():
    logger = logging.getLogger("pg_logical_repl")
    for handler in logger.handlers:
        handler.flush()
        if handler.stream is not sys.stderr:
            handler.stream.close()
        handler.close()
    logger.handlers = []
