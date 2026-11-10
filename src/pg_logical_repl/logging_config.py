"""JSON event logging; the service passes only redacted records."""

import logging
import os
import sys

_handlers = []
_previous = None


def configure(level="INFO", path=None):
    global _previous
    close()
    logger = logging.getLogger("pg_logical_repl")
    _previous = (logger.level, logger.propagate)
    logger.propagate = False
    logger.setLevel(level.upper())
    streams = [(sys.stderr, False)]
    if path:
        fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        os.fchmod(fd, 0o600)
        streams.append((os.fdopen(fd, "a"), True))
    for stream, owned in streams:
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        _handlers.append((handler, owned))


def close():
    global _previous
    logger = logging.getLogger("pg_logical_repl")
    while _handlers:
        handler, owned = _handlers.pop()
        logger.removeHandler(handler)
        try:
            handler.flush()
        finally:
            if owned:
                handler.stream.close()
            handler.close()
    if _previous is not None:
        logger.setLevel(_previous[0])
        logger.propagate = _previous[1]
        _previous = None
