# coding=utf-8
"""SQLAlchemy session."""

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateSchema

from kombu.utils.compat import register_after_fork

ModelBase = declarative_base()


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


class SessionManager(object):
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
            return create_engine(dburi, poolclass=NullPool)

    def create_session(self, dburi, schema=None, short_lived_sessions=False, **kwargs):
        engine = self.get_engine(dburi, **kwargs)
        engine = engine.execution_options(schema_translate_map={'celery_schema': schema})
        if self.forked:
            if short_lived_sessions or dburi not in self._sessions:
                self._sessions[dburi] = sessionmaker(bind=engine)
            return engine, self._sessions[dburi]
        else:
            return engine, sessionmaker(bind=engine)

    def prepare_models(self, engine, schema=None):
        if not self.prepared:
            if schema:
                with engine.connect() as connection:
                    connection.execute(CreateSchema(schema, if_not_exists=True))
                    connection.commit()
            ModelBase.metadata.create_all(engine)
            self.prepared = True
