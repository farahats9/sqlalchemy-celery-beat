# coding=utf-8
"""SQLAlchemy session."""

import time
from contextlib import contextmanager

from celery.utils.time import get_exponential_backoff_interval
from kombu.utils.compat import register_after_fork
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# from sqlalchemy.schema import CreateSchema  # does not work for SA 1.4


ModelBase = declarative_base()
PREPARE_MODELS_MAX_RETRIES = 10


@contextmanager
def session_cleanup(session):
    try:
        yield
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _after_fork_cleanup_session(session):
    session._after_fork()


class SessionManager:
    """Manage SQLAlchemy sessions."""

    def __init__(self):
        self._engines = {}
        self._sessions = {}
        self.forked = False
        self.prepared = False
        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_session)

    def _after_fork(self):
        self.forked = True

    def get_engine(self, dburi, **kwargs):
        if self.forked:
            try:
                return self._engines[dburi]
            except KeyError:
                engine = self._engines[dburi] = create_engine(dburi, **kwargs)
                return engine
        else:
            kwargs = {k: v for k, v in kwargs.items() if
                      not k.startswith('pool')}
            return create_engine(dburi, poolclass=NullPool, **kwargs)

    def create_session(self, dburi, schema=None, short_lived_sessions=False, **kwargs):
        engine = self.get_engine(dburi, future=True, **kwargs)
        engine = engine.execution_options(schema_translate_map={'celery_schema': schema})
        if self.forked:
            if short_lived_sessions or dburi not in self._sessions:
                self._sessions[dburi] = sessionmaker(bind=engine)
            return engine, self._sessions[dburi]
        return engine, sessionmaker(bind=engine)

    def prepare_models(self, engine, schema=None):
        if not self.prepared:
            # SQLAlchemy will check if the items exist before trying to
            # create them, which is a race condition. If it raises an error
            # in one iteration, the next may pass all the existence checks
            # and the call will succeed.
            retries = 0
            while True:
                try:
                    if schema:
                        with engine.begin() as connection:
                            connection.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};')

                    ModelBase.metadata.create_all(engine)
                except DatabaseError:
                    if retries < PREPARE_MODELS_MAX_RETRIES:
                        sleep_amount_ms = get_exponential_backoff_interval(
                            10, retries, 1000, True
                        )
                        time.sleep(sleep_amount_ms / 1000)
                        retries += 1
                    else:
                        raise
                else:
                    break

            self.prepared = True

    def session_factory(self, dburi, schema=None, **kwargs):
        engine, session = self.create_session(dburi, schema=schema, ** kwargs)
        self.prepare_models(engine)
        return session()
