import sys
from datetime import datetime
from pickle import dumps, loads
from unittest.mock import Mock, patch

import pytest
from celery import states, uuid
from celery.app.task import Context
from celery.exceptions import ImproperlyConfigured

from sqlalchemy_celery_beat import session

pytest.importorskip('sqlalchemy')

from celery.backends.database import retry

from sqlalchemy_celery_beat.session import (PREPARE_MODELS_MAX_RETRIES,  # noqa
                                            ModelBase, SessionManager,
                                            session_cleanup)

# import skip


class SomeClass:

    def __init__(self, data):
        self.data = data

    def __eq__(self, cmp):
        return self.data == cmp.data


class test_session_cleanup:

    def test_context(self):
        session = Mock(name='session')
        with session_cleanup(session):
            pass
        session.close.assert_called_with()

    def test_context_raises(self):
        session = Mock(name='session')
        with pytest.raises(KeyError):
            with session_cleanup(session):
                raise KeyError()
        session.rollback.assert_called_with()
        session.close.assert_called_with()


class test_SessionManager:

    def test_after_fork(self):
        s = SessionManager()
        assert not s.forked
        s._after_fork()
        assert s.forked

    @patch('sqlalchemy_celery_beat.session.create_engine')
    def test_get_engine_forked(self, create_engine):
        s = SessionManager()
        s._after_fork()
        engine = s.get_engine('dburi', foo=1)
        create_engine.assert_called_with('dburi', foo=1)
        assert engine is create_engine()
        engine2 = s.get_engine('dburi', foo=1)
        assert engine2 is engine

    @patch('sqlalchemy_celery_beat.session.create_engine')
    def test_get_engine_kwargs(self, create_engine):
        s = SessionManager()
        engine = s.get_engine('dbur', foo=1, pool_size=5)
        assert engine is create_engine()
        engine2 = s.get_engine('dburi', foo=1)
        assert engine2 is engine

    @patch('sqlalchemy_celery_beat.session.sessionmaker')
    def test_create_session_forked(self, sessionmaker):
        s = SessionManager()
        s.get_engine = Mock(name='get_engine')
        s._after_fork()
        engine, session = s.create_session('dburi', short_lived_sessions=True)
        sessionmaker.assert_called_with(bind=s.get_engine().execution_options(schema_translate_map={'celery_schema': None}), expire_on_commit=False)
        assert session is sessionmaker()
        sessionmaker.return_value = Mock(name='new')
        engine, session2 = s.create_session('dburi', short_lived_sessions=True)
        sessionmaker.assert_called_with(bind=s.get_engine().execution_options(schema_translate_map={'celery_schema': None}), expire_on_commit=False)
        assert session2 is not session
        sessionmaker.return_value = Mock(name='new2')
        engine, session3 = s.create_session(
            'dburi', short_lived_sessions=False)
        sessionmaker.assert_called_with(bind=s.get_engine().execution_options(schema_translate_map={'celery_schema': None}), expire_on_commit=False)
        assert session3 is session2

    def test_coverage_madness(self):
        prev, session.register_after_fork = (
            session.register_after_fork, None,
        )
        try:
            SessionManager()
        finally:
            session.register_after_fork = prev

    @patch('sqlalchemy_celery_beat.session.create_engine')
    def test_prepare_models_terminates(self, create_engine):
        """SessionManager.prepare_models has retry logic because the creation
        of database tables by multiple workers is racy. This test patches
        the used method to always raise, so we can verify that it does
        eventually terminate.
        """
        from sqlalchemy.dialects.sqlite import dialect
        from sqlalchemy.exc import DatabaseError

        if hasattr(dialect, 'dbapi'):
            # Method name in SQLAlchemy < 2.0
            sqlite = dialect.dbapi()
        else:
            # Newer method name in SQLAlchemy 2.0
            sqlite = dialect.import_dbapi()
        manager = SessionManager()
        engine = manager.get_engine('dburi')

        def raise_err(bind):
            raise DatabaseError("", "", [], sqlite.DatabaseError)

        patch_create_all = patch.object(
            ModelBase.metadata, 'create_all', side_effect=raise_err)

        with pytest.raises(DatabaseError), patch_create_all as mock_create_all:
            manager.prepare_models(engine)

        assert mock_create_all.call_count == PREPARE_MODELS_MAX_RETRIES + 1
