"""One PostgreSQL session lock shared by cron and admin summary execution."""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from inspect import signature
from sqlalchemy import text
from sqlalchemy.orm import Session

SUMMARY_LOCK = 72483011
_held = ContextVar("summary_lock_held", default=False)


class SummaryBusyError(RuntimeError):
    pass


@contextmanager
def summary_execution_lock(engine):
    if _held.get():
        yield
        return
    # A dedicated connection keeps the lock across pipeline session commits.
    with engine.connect() as connection:
        acquired = connection.execute(
            text("SELECT pg_try_advisory_lock(:key)"), {"key": SUMMARY_LOCK}
        ).scalar()
        connection.commit()
        if not acquired:
            raise SummaryBusyError("Another summary run is active; retry later.")
        try:
            token = _held.set(True)
            yield
        finally:
            _held.reset(token)
            connection.execute(
                text("SELECT pg_advisory_unlock(:key)"), {"key": SUMMARY_LOCK}
            )
            connection.commit()


def serialized_summary_run(function):
    @wraps(function)
    def run(*args, **kwargs):
        from ..database.base import get_session

        bound = signature(function).bind(*args, **kwargs)
        session = bound.arguments.get("session")
        owns_session = session is None
        if owns_session:
            session = get_session()
            bound.arguments["session"] = session
        try:
            # Unit-test doubles do not have a real DB connection.
            if isinstance(session, Session):
                with summary_execution_lock(session.get_bind()):
                    return function(*bound.args, **bound.kwargs)
            return function(*bound.args, **bound.kwargs)
        finally:
            if owns_session:
                session.close()

    return run
