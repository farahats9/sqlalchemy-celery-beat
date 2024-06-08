import math
import os
import sys
import time
from datetime import datetime, timedelta
from itertools import count
from time import monotonic

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

import pytest
from celery.schedules import crontab, schedule, solar
from celery.utils.time import make_aware

from sqlalchemy_celery_beat import schedulers
from sqlalchemy_celery_beat.clockedschedule import clocked
from sqlalchemy_celery_beat.models import (ClockedSchedule, CrontabSchedule,
                                           IntervalSchedule, Period,
                                           PeriodicTask, PeriodicTaskChanged,
                                           SolarSchedule)
from sqlalchemy_celery_beat.session import SessionManager, session_cleanup
from sqlalchemy_celery_beat.time_utils import NEVER_CHECK_TIMEOUT

_ids = count(0)


@pytest.fixture(autouse=True)
def no_multiprocessing_finalizers(patching):
    patching('multiprocessing.util.Finalize')
    patching('sqlalchemy_celery_beat.schedulers.Finalize')


class EntryTrackSave(schedulers.ModelEntry):

    def __init__(self, *args, **kwargs):
        self.saved = 0
        super().__init__(*args, **kwargs)

    def save(self):
        self.saved += 1
        super().save()


class EntrySaveRaises(schedulers.ModelEntry):

    def save(self):
        raise RuntimeError('this is expected')


class TrackingScheduler(schedulers.DatabaseScheduler):
    Entry = EntryTrackSave

    def __init__(self, *args, **kwargs):
        self.flushed = 0
        schedulers.DatabaseScheduler.__init__(self, *args, **kwargs)

    def sync(self):
        self.flushed += 1
        schedulers.DatabaseScheduler.sync(self)


class SchedulerCase:

    def create_model_interval(self, session, sch, **kwargs):
        interval = IntervalSchedule.from_schedule(session, sch)
        return self.create_model(schedule_model=interval, **kwargs)

    def create_model_crontab(self, session, sch, **kwargs):
        s = CrontabSchedule.from_schedule(session, sch)
        return self.create_model(schedule_model=s, **kwargs)

    def create_model_solar(self, session, sch, **kwargs):
        s = SolarSchedule.from_schedule(session, sch)
        return self.create_model(schedule_model=s, **kwargs)

    def create_model_clocked(self, session, sch, **kwargs):
        s = ClockedSchedule.from_schedule(session, sch)
        return self.create_model(schedule_model=s, one_off=True, **kwargs)

    def create_conf_entry(self):
        name = f'thefoo{next(_ids)}'
        return name, dict(
            task=f'djcelery.unittest.add{next(_ids)}',
            schedule=timedelta(0, 600),
            args=(),
            relative=False,
            kwargs={},
            options={'queue': 'extra_queue'}
        )

    def create_model(self, Model=PeriodicTask, **kwargs):
        entry = dict(
            name=f'thefoo{next(_ids)}',
            task=f'djcelery.unittest.add{next(_ids)}',
            args='[2, 2]',
            kwargs='{"callback": "foo"}',
            queue='xaz',
            routing_key='cpu',
            priority=1,
            headers='{"_schema_name": "foobar"}',
            exchange='foo',
        )
        return Model(**dict(entry, **kwargs))

    def create_interval_schedule(self, session):
        with session_cleanup(session):
            s = IntervalSchedule(every=10, period=Period.DAYS)
            session.add(s)
            session.commit()
            return s

    def create_crontab_schedule(self, session):
        with session_cleanup(session):
            s = CrontabSchedule(timezone='UTC')
            session.add(s)
            session.commit()
            return s


class test_DatabaseSchedulerFromAppConf(SchedulerCase):
    Scheduler = TrackingScheduler

    @pytest.fixture(autouse=True)
    def setup_scheduler(self, app):
        self.app = app
        self.entry_name, self.entry = self.create_conf_entry()
        self.app.conf.beat_dburi = 'sqlite:///tests/testing.db'
        Session = SessionManager()
        self.session = Session.session_factory(self.app.conf.beat_dburi)
        self.app.conf.beat_schedule = {self.entry_name: self.entry}
        self.m1 = PeriodicTask(name=self.entry_name, task=self.entry['task'],
                               schedule_model=self.create_interval_schedule(self.session))
        # print(self.m1)

    def test_constructor(self):
        s = self.Scheduler(app=self.app)

        assert isinstance(s._dirty, set)
        assert s._last_sync is None
        assert s.sync_every

    def test_periodic_task_model_enabled_schedule(self):
        s = self.Scheduler(app=self.app)
        sched = s.schedule
        assert len(sched) == 2
        assert 'celery.backend_cleanup' in sched
        assert self.entry_name in sched
        for n, e in sched.items():
            assert isinstance(e, s.Entry)
            if n == 'celery.backend_cleanup':
                assert e.options['expires'] == 12 * 3600
                assert e.model.expires is None
                assert e.model.expire_seconds == 12 * 3600

    def test_periodic_task_model_disabled_schedule(self):
        self.m1.enabled = False
        with session_cleanup(self.session):
            self.session.add(self.m1)
            self.session.commit()
        s = self.Scheduler(app=self.app)
        sched = s.schedule
        assert sched
        assert len(sched) == 1
        assert 'celery.backend_cleanup' in sched
        assert self.entry_name not in sched

    def test_periodic_task_model_schedule_type_change(self):
        self.m1.schedule_model = None
        self.m1.schedule_model = self.create_crontab_schedule(self.session)
        with session_cleanup(self.session):
            self.session.add(self.m1)
            self.session.commit()
            s = self.Scheduler(app=self.app)
            self.session.refresh(self.m1)
            assert self.m1.discriminator == 'intervalschedule'
            assert self.m1.discriminator != 'crontabschedule'


class test_DatabaseScheduler(SchedulerCase):
    Scheduler = TrackingScheduler

    @pytest.fixture(autouse=True)
    def setup_scheduler(self, app):
        self.app = app
        self.app.conf.beat_dburi = 'sqlite:///tests/testing.db'
        Session = SessionManager()
        self.session = Session.session_factory(self.app.conf.beat_dburi)
        self.app.conf.beat_schedule = {}

        with session_cleanup(self.session):
            self.m1 = self.create_model_interval(
                self.session,
                schedule(timedelta(seconds=10)))
            self.session.add(self.m1)
            self.session.flush()

            self.m2 = self.create_model_interval(
                self.session,
                schedule(timedelta(minutes=20)))
            self.session.add(self.m2)
            self.session.flush()

            self.m3 = self.create_model_crontab(
                self.session,
                crontab(minute='2,4,5'))
            self.session.add(self.m3)
            self.session.flush()

            self.m4 = self.create_model_solar(
                self.session,
                solar('solar_noon', 48.06, 12.86))
            self.session.add(self.m4)
            self.session.flush()

            dt_aware = make_aware(datetime(day=26,
                                           month=7,
                                           year=3000,
                                           hour=1,
                                           minute=0),
                                  ZoneInfo('UTC'))  # future time
            self.m6 = self.create_model_clocked(
                self.session,
                clocked(dt_aware)
            )
            self.session.add(self.m6)
            self.session.flush()

            # disabled, should not be in schedule
            m5 = self.create_model_interval(
                self.session,
                schedule(timedelta(seconds=1)))
            m5.enabled = False
            self.session.add(m5)
            self.session.flush()

            self.session.commit()
            self.session.refresh(self.m1)
            self.session.refresh(self.m2)
            self.session.refresh(self.m3)
            self.session.refresh(self.m4)
            self.session.refresh(self.m6)
            self.session.refresh(m5)
        self.s = self.Scheduler(app=self.app)

    def test_constructor(self):
        assert isinstance(self.s._dirty, set)
        assert self.s._last_sync is None
        assert self.s.sync_every

    def test_all_as_schedule(self):
        sched = self.s.schedule
        assert sched
        assert len(sched) == 6
        assert 'celery.backend_cleanup' in sched
        for n, e in sched.items():
            assert isinstance(e, self.s.Entry)

    def test_schedule_changed(self):
        self.m2.args = '[16, 16]'
        with session_cleanup(self.session):
            self.session.add(self.m2)
            self.session.commit()
            e2 = self.s.schedule[self.m2.name]
            assert e2.args == [16, 16]

            self.m1.args = '[32, 32]'
            self.session.add(self.m1)
            self.session.commit()
            e1 = self.s.schedule[self.m1.name]
            assert e1.args == [32, 32]
            e1 = self.s.schedule[self.m1.name]
            assert e1.args == [32, 32]

            self.session.delete(self.m3)
            self.session.commit()
            with pytest.raises(KeyError):
                self.s.schedule.__getitem__(self.m3.name)

    def test_should_sync(self):
        assert self.s.should_sync()
        self.s._last_sync = monotonic()
        assert not self.s.should_sync()
        self.s._last_sync -= (self.s.sync_every + 1)
        assert self.s.should_sync()

    def test_reserve(self):
        e1 = self.s.schedule[self.m1.name]
        self.s.schedule[self.m1.name] = self.s.reserve(e1)
        assert self.s.flushed == 1

        e2 = self.s.schedule[self.m2.name]
        self.s.schedule[self.m2.name] = self.s.reserve(e2)
        assert self.s.flushed == 1
        assert self.m2.name in self.s._dirty

    def test_sync_saves_last_run_at(self):
        e1 = self.s.schedule[self.m2.name]
        last_run = e1.last_run_at
        last_run2 = last_run - timedelta(days=1)
        e1.model.last_run_at = last_run2
        self.s._dirty.add(self.m2.name)
        self.s.sync()

        e2 = self.s.schedule[self.m2.name]
        assert e2.last_run_at == last_run2

    def test_sync_syncs_before_save(self):
        # Get the entry for m2
        e1 = self.s.schedule[self.m2.name]

        # Increment the entry (but make sure it doesn't sync)
        self.s._last_sync = monotonic()
        e2 = self.s.schedule[e1.name] = self.s.reserve(e1)
        assert self.s.flushed == 1

        # Fetch the raw object from db, change the args
        # and save the changes.
        with session_cleanup(self.session):
            m2 = self.session.get(PeriodicTask, self.m2.id)
            m2.args = '[16, 16]'
            self.session.add(m2)
            self.session.commit()

        # get_schedule should now see the schedule has changed.
        # and also sync the dirty objects.
        e3 = self.s.schedule[self.m2.name]
        assert self.s.flushed == 2
        assert e3.last_run_at == e2.last_run_at
        assert e3.args == [16, 16]

    def test_periodic_task_disabled_and_enabled(self):
        # Get the entry for m2
        e1 = self.s.schedule[self.m2.name]

        # Increment the entry (but make sure it doesn't sync)
        self.s._last_sync = monotonic()
        self.s.schedule[e1.name] = self.s.reserve(e1)
        assert self.s.flushed == 1

        # Fetch the raw object from db, change the args
        # and save the changes.
        with session_cleanup(self.session):
            m2 = self.session.get(PeriodicTask, self.m2.id)
            m2.enabled = False
            self.session.add(m2)
            self.session.commit()

            # get_schedule should now see the schedule has changed.
            # and remove entry for m2
            assert self.m2.name not in self.s.schedule
            assert self.s.flushed == 2

            m2.enabled = True
            self.session.add(m2)
            self.session.commit()

            # get_schedule should now see the schedule has changed.
            # and add entry for m2
            assert self.m2.name in self.s.schedule
            assert self.s.flushed == 3

    def test_periodic_task_disabled_while_reserved(self):
        # Get the entry for m2
        e1 = self.s.schedule[self.m2.name]

        # Increment the entry (but make sure it doesn't sync)
        self.s._last_sync = monotonic()
        e2 = self.s.schedule[e1.name] = self.s.reserve(e1)
        assert self.s.flushed == 1

        # Fetch the raw object from db, change the args
        # and save the changes.
        with session_cleanup(self.session):
            m2 = self.session.get(PeriodicTask, self.m2.id)
            m2.enabled = False
            self.session.add(m2)
            self.session.commit()

        # reserve is called because the task gets called from
        # tick after the database change is made
        self.s.reserve(e2)

        # get_schedule should now see the schedule has changed.
        # and remove entry for m2
        assert self.m2.name not in self.s.schedule
        assert self.s.flushed == 2

    def test_sync_not_dirty(self):
        self.s._dirty.clear()
        self.s.sync()

    def test_sync_object_gone(self):
        self.s._dirty.add('does-not-exist')
        self.s.sync()

    def test_sync_rollback_on_save_error(self):
        Session = SessionManager()
        self.s.schedule[self.m1.name] = EntrySaveRaises(self.m1, Session, app=self.app)
        self.s._dirty.add(self.m1.name)
        with pytest.raises(RuntimeError):
            self.s.sync()

    def test_update_scheduler_heap_invalidation(self, monkeypatch):
        # mock "schedule_changed" to always trigger update for
        # all calls to schedule, as a change may occur at any moment
        monkeypatch.setattr(self.s, 'schedule_changed', lambda: True)
        self.s.tick()

    def test_heap_size_is_constant(self, monkeypatch):
        # heap size is constant unless the schedule changes
        monkeypatch.setattr(self.s, 'schedule_changed', lambda: True)
        expected_heap_size = len(self.s.schedule.values())
        self.s.tick()
        assert len(self.s._heap) == expected_heap_size
        self.s.tick()
        assert len(self.s._heap) == expected_heap_size

    def test_scheduler_schedules_equality_on_change(self, monkeypatch):
        monkeypatch.setattr(self.s, 'schedule_changed', lambda: False)
        assert self.s.schedules_equal(self.s.schedule, self.s.schedule)

        monkeypatch.setattr(self.s, 'schedule_changed', lambda: True)
        assert not self.s.schedules_equal(self.s.schedule, self.s.schedule)

    def test_heap_always_return_the_first_item(self):
        interval = 10
        with session_cleanup(self.session):
            s1 = schedule(timedelta(seconds=interval))
            m1 = self.create_model_interval(self.session, s1, enabled=False)
            m1.last_run_at = self.app.now() - timedelta(seconds=interval + 2)
            self.session.add(m1)
            self.session.flush()

            s2 = schedule(timedelta(seconds=interval))
            m2 = self.create_model_interval(self.session, s2, enabled=True)
            m2.last_run_at = self.app.now() - timedelta(seconds=interval + 1)
            self.session.add(m2)
            self.session.flush()

            self.session.commit()
            self.session.refresh(m1)
            self.session.refresh(m2)
        Session = SessionManager()
        e1 = EntryTrackSave(m1, Session, self.app)
        # because the disabled task e1 runs first, e2 will never be executed
        e2 = EntryTrackSave(m2, Session, self.app)

        s = self.Scheduler(app=self.app)
        s.schedule.clear()
        s.schedule[e1.name] = e1
        s.schedule[e2.name] = e2

        tried = set()
        for _ in range(len(s.schedule) * 8):
            tick_interval = s.tick()
            if tick_interval and tick_interval > 0.0:
                tried.add(s._heap[0].entry.name)
                time.sleep(tick_interval)
                if s.should_sync():
                    s.sync()
        assert len(tried) == 1 and tried == {e1.name}

    def test_starttime_trigger(self, monkeypatch):
        # Ensure there is no heap block in case of new task with start_time
        with session_cleanup(self.session):
            self.session.query(PeriodicTask).delete()
            self.session.commit()
            s = self.Scheduler(app=self.app)
            assert not s._heap
            m1 = self.create_model_interval(self.session, schedule(timedelta(seconds=3)))
            self.session.add(m1)
            self.session.commit()
            s.tick()
            assert len(s._heap) == 2
            m2 = self.create_model_interval(
                self.session,
                schedule(timedelta(days=1)),
                start_time=make_aware(
                    datetime.now() + timedelta(seconds=2),
                    ZoneInfo('UTC')))
            self.session.add(m2)
            self.session.commit()
            s.tick()
            assert s._heap[0][2].name == m2.name
            assert len(s._heap) == 3
            assert s._heap[0]
            time.sleep(2)
            s.tick()
            assert s._heap[0]
            assert s._heap[0][2].name == m1.name


class test_models(SchedulerCase):

    @pytest.fixture(autouse=True)
    def setup_scheduler(self, app):
        self.app = app
        self.app.conf.beat_dburi = 'sqlite:///tests/testing.db'
        Session = SessionManager()
        self.session = Session.session_factory(self.app.conf.beat_dburi)

    def test_IntervalSchedule_unicode(self):
        assert (str(IntervalSchedule(every=1, period='seconds'))
                == 'every second')
        assert (str(IntervalSchedule(every=10, period='seconds'))
                == 'every 10 seconds')

    def test_CrontabSchedule_unicode(self):
        assert str(CrontabSchedule(
            minute=3,
            hour=3,
            day_of_week=None,
        )) == '3 3 * * * (m/h/dM/MY/d) UTC'
        assert str(CrontabSchedule(
            minute=3,
            hour=3,
            day_of_week='tue',
            day_of_month='*/2',
            month_of_year='4,6',
        )) == '3 3 */2 4,6 tue (m/h/dM/MY/d) UTC'

    def test_PeriodicTask_unicode_interval(self):
        p = self.create_model_interval(self.session, schedule(timedelta(seconds=10)))
        assert str(p) == f'{p.name}: every 10.0 seconds'

    def test_PeriodicTask_unicode_crontab(self):
        p = self.create_model_crontab(self.session,
                                      crontab(
                                          hour='4, 5',
                                          day_of_week='4, 5',
                                      ))
        assert str(p) == """{}: * 4,5 * * 4,5 (m/h/dM/MY/d) UTC""".format(
            p.name
        )

    def test_PeriodicTask_unicode_solar(self):
        p = self.create_model_solar(
            self.session,
            solar('solar_noon', 48.06, 12.86), name='solar_event'
        )
        assert str(p) == 'solar_event: {} ({}, {})'.format(
            'solar noon', '48.06', '12.86'
        )

    def test_PeriodicTask_unicode_clocked(self):
        time = make_aware(datetime.now(), ZoneInfo('UTC'))
        p = self.create_model_clocked(
            self.session,
            clocked(time), name='clocked_event'
        )
        assert str(p) == '{}: {}'.format(
            'clocked_event', str(time)
        )

    def test_PeriodicTask_schedule_property(self):
        p1 = self.create_model_interval(self.session, schedule(timedelta(seconds=10)))
        s1 = p1.schedule
        assert s1.run_every.total_seconds() == 10

        p2 = self.create_model_crontab(self.session,
                                       crontab(
                                           hour='4, 5',
                                           minute='10,20,30',
                                           day_of_month='1-7',
                                           month_of_year='*/3',
                                       ))
        s2 = p2.schedule
        assert s2.hour == {4, 5}
        assert s2.minute == {10, 20, 30}
        assert s2.day_of_week == {0, 1, 2, 3, 4, 5, 6}
        assert s2.day_of_month == {1, 2, 3, 4, 5, 6, 7}
        assert s2.month_of_year == {1, 4, 7, 10}

    def test_PeriodicTask_unicode_no_schedule(self):
        p = self.create_model()
        assert str(p) == f'{p.name}: no schedule'

    def test_CrontabSchedule_schedule(self):
        s = CrontabSchedule(
            minute='3, 7',
            hour='3, 4',
            day_of_week='*',
            day_of_month='1, 16',
            month_of_year='1, 7',
        )
        assert s.schedule.minute == {3, 7}
        assert s.schedule.hour == {3, 4}
        assert s.schedule.day_of_week == {0, 1, 2, 3, 4, 5, 6}
        assert s.schedule.day_of_month == {1, 16}
        assert s.schedule.month_of_year == {1, 7}

    def test_CrontabSchedule_long_schedule(self):
        s = CrontabSchedule(
            minute=str(list(range(60)))[1:-1],
            hour=str(list(range(24)))[1:-1],
            day_of_week=str(list(range(7)))[1:-1],
            day_of_month=str(list(range(1, 32)))[1:-1],
            month_of_year=str(list(range(1, 13)))[1:-1]
        )
        assert s.schedule.minute == set(range(60))
        assert s.schedule.hour == set(range(24))
        assert s.schedule.day_of_week == set(range(7))
        assert s.schedule.day_of_month == set(range(1, 32))
        assert s.schedule.month_of_year == set(range(1, 13))
        fields = [
            'minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year'
        ]
        for field in fields:
            str_length = len(str(getattr(s.schedule, field)))
            field_length = getattr(CrontabSchedule, field, None).type.length
            assert str_length <= field_length

    def test_SolarSchedule_schedule(self):
        s = SolarSchedule(event='solar_noon', latitude=48.06, longitude=12.86)
        dt = datetime(day=26, month=7, year=2050, hour=1, minute=0)
        dt_lastrun = make_aware(dt, ZoneInfo('UTC'))

        assert s.schedule is not None
        isdue, nextcheck = s.schedule.is_due(dt_lastrun)
        assert isdue is False  # False means task isn't due, but keep checking.
        assert (nextcheck > 0) and (isdue is False) or \
            (nextcheck == s.max_interval) and (isdue is True)

        s2 = SolarSchedule(event='solar_noon', latitude=48.06, longitude=12.86)
        dt2 = datetime(day=26, month=7, year=2000, hour=1, minute=0)
        dt2_lastrun = make_aware(dt2, ZoneInfo('UTC'))

        assert s2.schedule is not None
        isdue2, nextcheck2 = s2.schedule.is_due(dt2_lastrun)
        assert isdue2 is True  # True means task is due and should run.
        assert (nextcheck2 > 0) and (isdue2 is True) or \
            (nextcheck2 == s2.max_interval) and (isdue2 is False)

    def test_ClockedSchedule_schedule(self):
        due_datetime = make_aware(datetime(day=26,
                                           month=7,
                                           year=3000,
                                           hour=1,
                                           minute=0),
                                  ZoneInfo('UTC'))  # future time
        s = ClockedSchedule(clocked_time=due_datetime)
        dt = datetime(day=25, month=7, year=2050, hour=1, minute=0)
        dt_lastrun = make_aware(dt, ZoneInfo('UTC'))

        assert s.schedule is not None
        isdue, nextcheck = s.schedule.is_due(dt_lastrun)
        assert isdue is False  # False means task isn't due, but keep checking.
        assert (nextcheck > 0) and (isdue is False) or \
            (nextcheck == s.max_interval) and (isdue is True)

        due_datetime = make_aware(datetime.now(), ZoneInfo('UTC'))
        s = ClockedSchedule(clocked_time=due_datetime)
        dt2_lastrun = make_aware(datetime.now(), ZoneInfo('UTC'))

        assert s.schedule is not None
        isdue2, nextcheck2 = s.schedule.is_due(dt2_lastrun)
        assert isdue2 is True  # True means task is due and should run.
        assert (nextcheck2 == NEVER_CHECK_TIMEOUT) and (isdue2 is True)

    def test_PeriodicTask_specifyjoin(self):
        p = self.create_model_interval(self.session, schedule(timedelta(seconds=3)))
        c = self.create_model_crontab(self.session, crontab(minute="3", hour="3"))

        self.session.add(p)
        self.session.add(c)
        self.session.commit()

        p = self.session.query(PeriodicTask).first()

        assert p.schedule_id == 1
        assert c.schedule_id == 1

        assert p.discriminator == 'intervalschedule'
        assert p.model_crontabschedule is None
        assert p.model_intervalschedule is not None

        assert c.discriminator == 'crontabschedule'
        assert c.model_crontabschedule is not None
        assert c.model_intervalschedule is None

    def test_PeriodicTask_defaults(self):
        with session_cleanup(self.session):
            interval = IntervalSchedule.from_schedule(self.session, schedule(timedelta(seconds=3)))
            p = PeriodicTask(
                name=f'thefoo{next(_ids)}',
                task=f'djcelery.unittest.add{next(_ids)}',
                schedule_model=interval
            )
            assert p.args == '[]'
            assert p.kwargs == '{}'
            assert p.headers == '{}'
            assert p.one_off == False
            assert p.enabled == True
            assert p.total_run_count == 0

            self.session.add(p)
            self.session.flush()
            assert p.args == '[]'
            assert p.kwargs == '{}'
            assert p.headers == '{}'
            assert p.one_off == False
            assert p.enabled == True
            assert p.total_run_count == 0

            self.session.commit()
            assert p.args == '[]'
            assert p.kwargs == '{}'
            assert p.headers == '{}'
            assert p.one_off == False
            assert p.enabled == True
            assert p.total_run_count == 0


class test_model_PeriodicTaskChanged(SchedulerCase):

    @pytest.fixture(autouse=True)
    def setup_scheduler(self, app):
        self.app = app
        self.app.conf.beat_dburi = 'sqlite:///tests/testing.db'
        Session = SessionManager()
        self.session = Session.session_factory(self.app.conf.beat_dburi)

    def test_track_changes(self):
        with session_cleanup(self.session):
            assert PeriodicTaskChanged.last_change(self.session) is None
            m1 = self.create_model_interval(self.session, schedule(timedelta(seconds=10)))
            self.session.add(m1)
            self.session.commit()
            x = PeriodicTaskChanged.last_change(self.session)
            assert x
            m1.args = '(23, 24)'
            self.session.add(m1)
            self.session.commit()
            y = PeriodicTaskChanged.last_change(self.session)
            assert y
            assert y > x
