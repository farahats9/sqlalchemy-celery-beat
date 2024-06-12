import datetime
import os

try:
    from zoneinfo import ZoneInfo, available_timezones
except ImportError:
    from backports.zoneinfo import available_timezones, ZoneInfo

import pytest
from celery import schedules
from celery.utils.time import make_aware

from sqlalchemy_celery_beat.models import (ClockedSchedule, CrontabSchedule,
                                           IntervalSchedule, Period,
                                           SolarEvent)
from sqlalchemy_celery_beat.session import SessionManager, session_cleanup


class TestMixin:

    @pytest.fixture(autouse=True)
    def setup_scheduler(self, app):
        self.app = app
        self.app.conf.beat_dburi = 'sqlite:///tests/testing.db'
        Session = SessionManager()
        self.session = Session.session_factory(self.app.conf.beat_dburi)


class TestDuplicatesMixin:
    def _test_duplicate_schedules(self, cls, session, kwargs, schedule=None):
        with session_cleanup(session):
            sched1 = cls(**kwargs)
            session.add(sched1)
            session.add(cls(**kwargs))
            session.commit()
            count = session.query(cls).filter_by(**kwargs).count()
            assert count == 2
            # try to create a duplicate schedule from a celery schedule
            if schedule is None:
                schedule = sched1.schedule
            sched3 = cls.from_schedule(session, schedule)
            # the schedule should be the first of the 2 previous duplicates
            assert sched3 == sched1
            # and the duplicates should not be deleted !
            count2 = session.query(cls).filter_by(**kwargs).count()
            assert count2 == 2


class test_CrontabScheduleTestCase(TestDuplicatesMixin, TestMixin):
    FIRST_VALID_TIMEZONE = available_timezones().pop()

    def test_duplicate_schedules(self):
        # See: https://github.com/celery/django-celery-beat/issues/322
        kwargs = {
            "minute": "*",
            "hour": "4",
            "day_of_week": "*",
            "day_of_month": "*",
            "month_of_year": "*",
            "day_of_week": "*",
        }
        schedule = schedules.crontab(hour="4")
        self._test_duplicate_schedules(CrontabSchedule, self.session, kwargs, schedule)


class test_SolarScheduleTestCase:
    EVENT_CHOICES = SolarEvent

    def test_celery_solar_schedules_included_as_event_choices(self):
        """Make sure that all Celery solar schedules are included
        in SolarSchedule `event` field choices, keeping synchronized
        Celery solar events with `django-celery-beat` supported solar
        events.

        This test is necessary because Celery solar schedules are
        hardcoded at models so that Django can discover their translations.
        """
        event_choices_values = [event.value for event in self.EVENT_CHOICES]
        for solar_event in schedules.solar._all_events:
            assert solar_event in event_choices_values

        for event_choice in event_choices_values:
            assert event_choice in schedules.solar._all_events


class test_IntervalScheduleTestCase(TestDuplicatesMixin, TestMixin):

    def test_duplicate_schedules(self):
        kwargs = {'every': 1, 'period': Period.SECONDS}
        schedule = schedules.schedule(run_every=1.0)
        self._test_duplicate_schedules(IntervalSchedule, self.session, kwargs, schedule)


class test_ClockedScheduleTestCase(TestDuplicatesMixin, TestMixin):

    @pytest.fixture(autouse=False)
    def set_timezone(self, app):
        self.app = app
        self.app.conf.timezone = 'Africa/Cairo'

    def test_duplicate_schedules(self):
        now = make_aware(datetime.datetime.now(datetime.UTC), ZoneInfo('UTC'))
        kwargs = {'clocked_time': now}
        self._test_duplicate_schedules(ClockedSchedule, self.session, kwargs)

    # IMPORTANT: we must have a valid timezone (not UTC) for accurate testing
    def test_timezone_format(self, set_timezone):
        """Ensure scheduled time is not shown in UTC when timezone is used"""
        tz_info = datetime.datetime.now(ZoneInfo(self.app.conf.timezone))
        with session_cleanup(self.session):
            schedule = self.session.query(ClockedSchedule).filter_by(clocked_time=tz_info).first()
            if not schedule:
                schedule = ClockedSchedule(clocked_time=tz_info)
                self.session.add(schedule)
                self.session.commit()
            # testnig str(schedule) calls make_aware() internally
            assert str(schedule.clocked_time) == str(schedule)
