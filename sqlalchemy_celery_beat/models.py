# coding=utf-8
# The generic foreign key is implemented after this example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/generic_associations/generic_fk.html
import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo
import enum
import sqlalchemy as sa
from celery import schedules
from celery.utils.log import get_logger
from sqlalchemy import event
from sqlalchemy.future import Connection
from sqlalchemy.orm import (Session, backref, foreign, relationship, remote,
                            validates)
from sqlalchemy.sql import insert, select, update

from .clockedschedule import clocked
from .session import ModelBase
from .tzcrontab import TzAwareCrontab

logger = get_logger('sqlalchemy_celery_beat.models')


def cronexp(field):
    """Representation of cron expression."""
    return field and str(field).replace(' ', '') or '*'


class ModelMixin(object):

    @classmethod
    def create(cls, **kw):
        return cls(**kw)

    def update(self, **kw):
        for attr, value in kw.items():
            setattr(self, attr, value)
        return self


class Period(str, enum.Enum):
    DAYS = 'days'
    HOURS = 'hours'
    MINUTES = 'minutes'
    SECONDS = 'seconds'
    MICROSECONDS = 'microseconds'


class SolarEvent(str, enum.Enum):
    DAWN_ASTRONOMICAL = 'dawn_astronomical'
    DAWN_CIVIL = 'dawn_civil'
    DAWN_NAUTICAL = 'dawn_nautical'
    DUSK_ASTRONOMICAL = 'dusk_astronomical'
    DUSK_CIVIL = 'dusk_civil'
    DUSK_NAUTICAL = 'dusk_nautical'
    SOLAR_NOON = 'solar_noon'
    SUNRISE = 'sunrise'
    SUNSET = 'sunset'


class PeriodicTaskChanged(ModelBase, ModelMixin):
    """Helper table for tracking updates to periodic tasks."""
    __table_args__ = {
        'sqlite_autoincrement': False,
        'schema': 'celery_schema'
    }

    id = sa.Column(sa.Integer, primary_key=True)
    last_update = sa.Column(
        sa.DateTime(timezone=True), nullable=False, default=dt.datetime.now)

    @classmethod
    def changed(cls, mapper, connection, target):
        """
        :param mapper: the Mapper which is the target of this event
        :param connection: the Connection being used
        :param target: the mapped instance being persisted
        """
        if not target.no_changes:
            cls.update_changed(mapper, connection, target)

    @classmethod
    def update_changed(cls, mapper, connection: Connection, target):
        """
        :param mapper: the Mapper which is the target of this event
        :param connection: the Connection being used
        :param target: the mapped instance being persisted
        """
        logger.info('Database last time set to now')
        s = connection.execute(select(PeriodicTaskChanged).
                               where(PeriodicTaskChanged.id == 1).limit(1))
        if not s:
            s = connection.execute(insert(PeriodicTaskChanged),
                                   last_update=dt.datetime.now())
        else:
            s = connection.execute(update(PeriodicTaskChanged).
                                   where(PeriodicTaskChanged.id == 1).
                                   values(last_update=dt.datetime.now()))

    @classmethod
    def update_from_session(cls, session: Session, commit: bool = True):
        """
        :param session: the Session to use
        :param commit: commit the session if set to true
        """
        connection = session.connection()
        cls.update_changed(None, connection, None)
        if commit:
            connection.commit()

    @classmethod
    def last_change(cls, session):
        periodic_tasks = session.query(PeriodicTaskChanged).get(1)
        if periodic_tasks:
            return periodic_tasks.last_update


class PeriodicTask(ModelBase, ModelMixin):

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    # name
    name = sa.Column(sa.String(255), unique=True)
    # task name
    task = sa.Column(sa.String(255))

    args = sa.Column(sa.Text(), default='[]')
    kwargs = sa.Column(sa.Text(), default='{}')
    # queue for celery
    queue = sa.Column(sa.String(255))
    # exchange for celery
    exchange = sa.Column(sa.String(255))
    # routing_key for celery
    routing_key = sa.Column(sa.String(255))
    priority = sa.Column(sa.Integer())
    expires = sa.Column(sa.DateTime(timezone=True))

    one_off = sa.Column(sa.Boolean(), default=False)
    start_time = sa.Column(sa.DateTime(timezone=True))
    enabled = sa.Column(sa.Boolean(), default=True)
    last_run_at = sa.Column(sa.DateTime(timezone=True))
    total_run_count = sa.Column(sa.Integer(), nullable=False, default=0)

    date_changed = sa.Column(sa.DateTime(timezone=True),
                             default=dt.datetime.now, onupdate=dt.datetime.now)
    description = sa.Column(sa.Text(), default='')

    no_changes = False

    discriminator = sa.Column(sa.String(50), nullable=False)
    """Refers to the type of parent."""

    schedule_id = sa.Column(sa.Integer(), nullable=False)
    """Refers to the primary key of the parent.

    This could refer to any table.
    """
    @property
    def schedule_model(self):
        """Provides in-Python access to the "parent" by choosing
        the appropriate relationship.

        """
        return getattr(self, "model_%s" % self.discriminator)

    @schedule_model.setter
    def schedule_model(self, value):
        if value is not None:
            self.schedule_id = value.id
            self.discriminator = value.discriminator
            setattr(self, "model_%s" % value.discriminator, value)

    @classmethod
    def validate_before_insert(cls, mapper, connection, target):
        if isinstance(target.schedule_model, ClockedSchedule) and not target.one_off:
            raise ValueError("one_off must be True for clocked schedule")

    def __repr__(self):
        fmt = '{0.name}: {0.schedule_model}'
        return fmt.format(self)

    @property
    def task_name(self):
        return self.task

    @task_name.setter
    def task_name(self, value):
        self.task = value

    @property
    def schedule(self):
        if self.schedule_model:
            return self.schedule_model.schedule
        raise ValueError('{} schedule is None!'.format(self.name))

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class ScheduleModel:
    """ScheduleModel mixin, inherited by all schedule classes

    """


@event.listens_for(ScheduleModel, "mapper_configured", propagate=True)
def setup_listener(mapper, class_):
    name = class_.__name__
    discriminator = name.lower()
    class_.periodic_tasks = relationship(
        PeriodicTask,
        primaryjoin=sa.and_(
            class_.id == foreign(remote(PeriodicTask.schedule_id)),
            PeriodicTask.discriminator == discriminator,
        ),
        backref=backref(
            "model_%s" % discriminator,
            primaryjoin=remote(class_.id) == foreign(PeriodicTask.schedule_id),
            viewonly=True
        ),
        overlaps='periodic_tasks',
        cascade="all, delete-orphan"
    )

    @event.listens_for(class_.periodic_tasks, "append")
    def append_periodic_tasks(target, value, initiator):
        value.discriminator = discriminator

    @property
    def get_discriminator(self):
        return self.__class__.__name__.lower()

    class_.discriminator = get_discriminator


class IntervalSchedule(ScheduleModel, ModelBase):

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    every = sa.Column(sa.Integer, nullable=False)
    period = sa.Column(sa.Enum(Period, values_callable=lambda obj: [e.value for e in obj]))

    def __repr__(self):
        if self.every == 1:
            return 'every {0}'.format(self.period_singular)
        return 'every {0} {1}'.format(self.every, self.period)

    @property
    def schedule(self):
        return schedules.schedule(
            dt.timedelta(**{self.period: self.every}),
            # nowfun=lambda: make_aware(now())
            # nowfun=dt.datetime.now
        )

    @classmethod
    def from_schedule(cls, session, schedule, period=Period.SECONDS):
        every = max(schedule.run_every.total_seconds(), 0)
        model = session.query(IntervalSchedule).filter_by(
            every=every, period=period).first()
        if not model:
            model = cls(every=every, period=period)
            session.add(model)
            session.commit()
        return model

    @property
    def period_singular(self):
        return self.period[:-1]


class CrontabSchedule(ScheduleModel, ModelBase):

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    minute = sa.Column(sa.String(60 * 4), default='*')
    hour = sa.Column(sa.String(24 * 4), default='*')
    day_of_week = sa.Column(sa.String(64), default='*')
    day_of_month = sa.Column(sa.String(31 * 4), default='*')
    month_of_year = sa.Column(sa.String(64), default='*')
    timezone = sa.Column(sa.String(64), default='UTC')

    def __repr__(self):
        return '{0} {1} {2} {3} {4} (m/h/d/dM/MY) {5}'.format(
            cronexp(self.minute), cronexp(self.hour),
            cronexp(self.day_of_week), cronexp(self.day_of_month),
            cronexp(self.month_of_year), str(self.timezone)
        )

    @property
    def schedule(self):
        return TzAwareCrontab(
            minute=self.minute,
            hour=self.hour, day_of_week=self.day_of_week,
            day_of_month=self.day_of_month,
            month_of_year=self.month_of_year,
            tz=ZoneInfo(self.timezone)
        )

    @classmethod
    def from_schedule(cls, session, schedule):
        spec = {
            'minute': schedule._orig_minute,
            'hour': schedule._orig_hour,
            'day_of_week': schedule._orig_day_of_week,
            'day_of_month': schedule._orig_day_of_month,
            'month_of_year': schedule._orig_month_of_year,
        }
        if schedule.tz:
            spec.update({
                'timezone': schedule.tz.key
            })
        model = session.query(CrontabSchedule).filter_by(**spec).first()
        if not model:
            model = cls(**spec)
            session.add(model)
            session.commit()
        return model


class SolarSchedule(ScheduleModel, ModelBase):

    __table_args__ = (
        sa.UniqueConstraint('event', 'latitude', 'longitude'),
        {
            'sqlite_autoincrement': True,
            'schema': 'celery_schema'
        }
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    event = sa.Column(sa.Enum(SolarEvent, values_callable=lambda obj: [e.value for e in obj]))
    latitude = sa.Column(sa.DECIMAL(precision=9, scale=6, decimal_return_scale=6, asdecimal=False))
    longitude = sa.Column(sa.DECIMAL(precision=9, scale=6, decimal_return_scale=6, asdecimal=False))

    @property
    def schedule(self):
        return schedules.solar(
            self.event,
            self.latitude,
            self.longitude,
            nowfun=dt.datetime.now
        )

    @classmethod
    def from_schedule(cls, session, schedule):
        spec = {
            'event': schedule.event,
            'latitude': schedule.lat,
            'longitude': schedule.lon
        }
        model = session.query(SolarSchedule).filter_by(**spec).first()
        if not model:
            model = cls(**spec)
            session.add(model)
            session.commit()
        return model

    def __repr__(self):
        return '{0} ({1}, {2})'.format(
            self.event,
            self.latitude,
            self.longitude
        )


class ClockedSchedule(ScheduleModel, ModelBase):

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    clocked_time = sa.Column(sa.DateTime(timezone=True))

    def __repr__(self):
        return f'{self.clocked_time}'

    @property
    def schedule(self):
        c = clocked(clocked_time=self.clocked_time)
        return c

    @classmethod
    def from_schedule(cls, session, schedule):
        spec = {'clocked_time': schedule.clocked_time}
        model = session.query(ClockedSchedule).filter_by(**spec).first()
        if not model:
            model = cls(**spec)
            session.add(model)
            session.commit()
        return model


event.listen(PeriodicTask, 'after_insert', PeriodicTaskChanged.update_changed)
event.listen(PeriodicTask, 'after_delete', PeriodicTaskChanged.update_changed)
event.listen(PeriodicTask, 'after_update', PeriodicTaskChanged.changed)
event.listen(PeriodicTask, 'before_insert', PeriodicTask.validate_before_insert)
event.listen(ScheduleModel, 'after_delete', PeriodicTaskChanged.update_changed, propagate=True)
event.listen(ScheduleModel, 'after_update', PeriodicTaskChanged.update_changed, propagate=True)
