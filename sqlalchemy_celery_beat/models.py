# coding=utf-8
# The generic foreign key is implemented after this example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/generic_associations/generic_fk.html
import re
import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo
try:
    from zoneinfo import available_timezones
except ImportError:
    from backports.zoneinfo import available_timezones
import enum
import sqlalchemy as sa
from celery import schedules
from celery.utils.log import get_logger
from celery.utils.time import maybe_make_aware, make_aware
from sqlalchemy import event
from sqlalchemy.future import Connection
from sqlalchemy.orm import (Session, backref, foreign, relationship, remote,
                            validates)
from sqlalchemy.sql import insert, select, update

from .clockedschedule import clocked
from .session import ModelBase
from .tzcrontab import TzAwareCrontab

logger = get_logger('sqlalchemy_celery_beat.models')


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
    DAWN_NAUTICAL = 'dawn_nautical'
    DAWN_CIVIL = 'dawn_civil'
    SUNRISE = 'sunrise'
    SOLAR_NOON = 'solar_noon'
    SUNSET = 'sunset'
    DUSK_CIVIL = 'dusk_civil'
    DUSK_NAUTICAL = 'dusk_nautical'
    DUSK_ASTRONOMICAL = 'dusk_astronomical'


class PeriodicTaskChanged(ModelBase, ModelMixin):
    """Helper table for tracking updates to periodic tasks."""
    __table_args__ = {
        'sqlite_autoincrement': False,
        'schema': 'celery_schema'
    }

    id = sa.Column(sa.Integer, primary_key=True)
    last_update = sa.Column(
        sa.DateTime(timezone=True), nullable=False, default=lambda: maybe_make_aware(dt.datetime.utcnow()))

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
                                   last_update=maybe_make_aware(dt.datetime.utcnow()))
        else:
            s = connection.execute(update(PeriodicTaskChanged).
                                   where(PeriodicTaskChanged.id == 1).
                                   values(last_update=maybe_make_aware(dt.datetime.utcnow())))

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

    __table_args__ = (
        sa.CheckConstraint(sa.column('priority').between(0, 255)),
        {
            'sqlite_autoincrement': True,
            'schema': 'celery_schema'
        }
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    # name
    name = sa.Column(sa.String(255), unique=True, nullable=False,
                     doc='Name',
                     comment='Short Description For This Task')
    # task name
    task = sa.Column(sa.String(255), nullable=False,
                     doc='Task Name',
                     comment='The Name of the Celery Task that Should be Run.  '
                     '(Example: "proj.tasks.import_contacts")')

    args = sa.Column(sa.Text(), default='[]', nullable=False,
                     doc='Positional Arguments',
                     comment='JSON encoded positional arguments '
                     '(Example: ["arg1", "arg2"])')
    kwargs = sa.Column(sa.Text(), default='{}', nullable=False,
                       doc='Keyword Arguments',
                       comment='JSON encoded keyword arguments '
                       '(Example: {"argument": "value"})')
    # queue for celery
    queue = sa.Column(sa.String(255),
                      doc='Queue Override',
                      comment='Queue defined in CELERY_TASK_QUEUES. '
                      'Leave None for default queuing.')
    # exchange for celery
    exchange = sa.Column(sa.String(255), doc='Exchange',
                         comment='Override Exchange for low-level AMQP routing')
    # routing_key for celery
    routing_key = sa.Column(sa.String(255), doc='Routing Key',
                            comment='Override Routing Key for low-level AMQP routing')
    headers = sa.Column(sa.Text,
                        default='{}',
                        doc='AMQP Message Headers',
                        comment='JSON encoded message headers for the AMQP message.',
                        )
    priority = sa.Column(sa.Integer(),
                         doc='Priority',
                         comment='Priority Number between 0 and 255. '
                         'Supported by: RabbitMQ, Redis (priority reversed, 0 is highest).')
    expires = sa.Column(sa.DateTime(timezone=True),
                        doc='Expires Datetime',
                        comment='Datetime after which the schedule will no longer '
                        'trigger the task to run')
    expire_seconds = sa.Column(sa.Integer,
                               doc='Expires timedelta with seconds',
                               comment='Timedelta with seconds which the schedule will no longer '
                               'trigger the task to run')

    one_off = sa.Column(sa.Boolean(), default=False, nullable=False,
                        doc='One-off Task',
                        comment='If True, the schedule will only run the task a single time')
    start_time = sa.Column(sa.DateTime(timezone=True),
                           doc='Start Datetime',
                           comment='Datetime when the schedule should begin '
                           'triggering the task to run')
    enabled = sa.Column(sa.Boolean(), default=True, nullable=False,
                        doc='Enabled',
                        comment='Set to False to disable the schedule')
    last_run_at = sa.Column(sa.DateTime(timezone=True), doc='Last Run Datetime',
                            comment='Datetime that the schedule last triggered the task to run. ')
    total_run_count = sa.Column(sa.Integer(), nullable=False, default=0,
                                doc='Total Run Count',
                                comment='Running count of how many times the schedule '
                                'has triggered the task')

    date_changed = sa.Column(sa.DateTime(timezone=True),
                             default=lambda: maybe_make_aware(dt.datetime.utcnow()),
                             onupdate=lambda: maybe_make_aware(dt.datetime.utcnow()),
                             doc='Last Modified',
                             comment='Datetime that this PeriodicTask was last modified')
    description = sa.Column(sa.Text(), default='', doc='Description',
                            comment='Detailed description about the details of this Periodic Task')

    no_changes = False

    discriminator = sa.Column(sa.String(20), nullable=False, doc='Schedule Name',
                              comment='Lower case name of the schedule class. ')
    """Refers to the type of parent."""

    schedule_id = sa.Column(sa.Integer(), nullable=False, doc='Schedule ID',
                            comment='ID of the schedule model object. ')
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

    @staticmethod
    def before_insert_or_update(mapper, connection, target):
        if target.enabled and isinstance(target.schedule_model, ClockedSchedule) and not target.one_off:
            raise ValueError("one_off must be True for clocked schedule")
        if target.expire_seconds is not None and target.expires:
            raise ValueError('Only one can be set, in expires and expire_seconds')

    def __repr__(self):
        fmt = '{0.name}: {0.schedule_model}'
        return fmt.format(self)

    @property
    def expires_(self):
        return self.expires or self.expire_seconds

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

    __table_args__ = (
        sa.CheckConstraint(sa.column('every') >= 1),
        {
            'sqlite_autoincrement': True,
            'schema': 'celery_schema'
        }
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    every = sa.Column(
        sa.Integer,
        nullable=False,
        doc='Number of Periods',
        comment='Number of interval periods to wait before '
        'running the task again'
    )
    period = sa.Column(
        sa.Enum(Period, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        doc='Interval Period',
        comment='The type of period between task runs (Example: days)'
    )

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
    minute = sa.Column(
        sa.String(60 * 4),
        nullable=False,
        default='*',
        doc='Minute(s)',
        comment='Cron Minutes to Run. Use "*" for "all". (Example: "0,30")'
    )
    hour = sa.Column(
        sa.String(24 * 4),
        nullable=False,
        default='*',
        doc='Hour(s)',
        comment='Cron Hours to Run. Use "*" for "all". (Example: "8,20")'
    )
    day_of_week = sa.Column(
        sa.String(64),
        nullable=False,
        default='*',
        doc='Day(s) Of The Week',
        comment='Cron Days Of The Week to Run. Use "*" for "all", Sunday '
        'is 0 or 7, Monday is 1. (Example: "0,5")'
    )
    day_of_month = sa.Column(
        sa.String(31 * 4),
        nullable=False,
        default='*',
        doc='Day(s) Of The Month',
        comment='Cron Days Of The Month to Run. Use "*" for "all". '
        '(Example: "1,15")'
    )
    month_of_year = sa.Column(
        sa.String(64),
        nullable=False,
        default='*',
        doc='Month(s) Of The Year',
        comment='Cron Months (1-12) Of The Year to Run. Use "*" for "all". '
        '(Example: "1,12")'
    )
    timezone = sa.Column(
        sa.String(64),
        nullable=False,
        default='UTC',
        doc='Cron Timezone',
        comment='Timezone to Run the Cron Schedule on. Default is UTC.'
    )

    def __repr__(self):
        return '{0} {1} {2} {3} {4} (m/h/d/dM/MY) {5}'.format(
            self.cronexp(self.minute), self.cronexp(self.hour),
            self.cronexp(self.day_of_week), self.cronexp(self.day_of_month),
            self.cronexp(self.month_of_year), str(self.timezone)
        )

    @staticmethod
    def aware_crontab(obj):
        return TzAwareCrontab(
            minute=obj.minute,
            hour=obj.hour, day_of_week=obj.day_of_week,
            day_of_month=obj.day_of_month,
            month_of_year=obj.month_of_year,
            tz=ZoneInfo(obj.timezone)
        )

    @property
    def schedule(self):
        return self.aware_crontab(self)

    @staticmethod
    def cronexp(value):
        return value and re.sub(r"[\s\[\]\{\}]", '', str(value))

    @classmethod
    def from_schedule(cls, session, schedule):
        spec = {
            'minute': cls.cronexp(schedule._orig_minute),
            'hour': cls.cronexp(schedule._orig_hour),
            'day_of_week': cls.cronexp(schedule._orig_day_of_week),
            'day_of_month': cls.cronexp(schedule._orig_day_of_month),
            'month_of_year': cls.cronexp(schedule._orig_month_of_year),
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

    @staticmethod
    def before_insert_or_update(mapper, connection, target):
        try:
            # Test the object to make sure it is valid before saving to DB
            CrontabSchedule.aware_crontab(target).remaining_estimate(dt.datetime.utcnow())
        except Exception as exc:
            raise ValueError(f"Could not parse cron values: {str(exc)}") from exc

    @validates('timezone')
    def validate_crontab(self, key, value):
        if value not in available_timezones():
            raise ValueError(f'Timezone "{value}"  is not found in available timezones')
        return value

    @validates('minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year')
    def validate_values(self, key, value):
        value = self.cronexp(value)
        return value


class SolarSchedule(ScheduleModel, ModelBase):

    __table_args__ = (
        sa.UniqueConstraint('event', 'latitude', 'longitude'),
        sa.CheckConstraint(sa.column('latitude').between(-90, 90)),
        sa.CheckConstraint(sa.column('longitude').between(-180, 180)),
        {
            'sqlite_autoincrement': True,
            'schema': 'celery_schema'
        }
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    event = sa.Column(
        sa.Enum(SolarEvent, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        doc='Solar Event',
        comment='The type of solar event when the job should run'
    )
    latitude = sa.Column(
        sa.Numeric(precision=9, scale=6, decimal_return_scale=6, asdecimal=False),
        nullable=False,
        doc='Latitude',
        comment='Run the task when the event happens at this latitude'
    )
    longitude = sa.Column(
        sa.Numeric(precision=9, scale=6, decimal_return_scale=6, asdecimal=False),
        nullable=False,
        doc='Longitude',
        comment='Run the task when the event happens at this longitude'
    )

    @property
    def schedule(self):
        return schedules.solar(
            self.event,
            self.latitude,
            self.longitude,
            nowfun=lambda: maybe_make_aware(dt.datetime.utcnow())
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

    def strip_ms(self):
        """ Convenience function to remove microseconds,
        this should help reduce number of clockedschedules in DB
        if you have too many.
        ex:
            s = ClockedSchedule(clocked_time=datetime.now() + timedelta(hours=1))
            # now your clocked_time looks like this: 2023-06-19 03:20:23.807020
            s.strip_ms()
            # now your clocked_time looks like this: 2023-06-19 03:20:23.000000
            session.add(s)
        """
        self.clocked_time = self.clocked_time.replace(microsecond=0)


event.listen(PeriodicTask, 'after_insert', PeriodicTaskChanged.update_changed)
event.listen(PeriodicTask, 'after_delete', PeriodicTaskChanged.update_changed)
event.listen(PeriodicTask, 'after_update', PeriodicTaskChanged.changed)
event.listen(PeriodicTask, 'before_insert', PeriodicTask.before_insert_or_update)
event.listen(PeriodicTask, 'before_update', PeriodicTask.before_insert_or_update)
event.listen(ScheduleModel, 'after_delete', PeriodicTaskChanged.update_changed, propagate=True)
event.listen(ScheduleModel, 'after_update', PeriodicTaskChanged.update_changed, propagate=True)
event.listen(CrontabSchedule, 'before_insert', CrontabSchedule.before_insert_or_update)
event.listen(CrontabSchedule, 'before_update', CrontabSchedule.before_insert_or_update)
