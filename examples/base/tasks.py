# coding=utf-8
"""
Ready::

    $ pip install -r requirements.txt

Run Worker::

    # console 1
    $ cd examples/base

    # Celery < 5.0
    $ celery worker -A tasks:celery -l info

    # Celery >= 5.0
    $ celery -A tasks:celery worker -l info

Run Beat::

    # console 2
    $ cd examples/base

    # Celery < 5.0
    $ celery beat -A tasks:celery -S tasks:DatabaseScheduler -l info

    # Celery >= 5.0
    $ celery -A tasks:celery beat -S tasks:DatabaseScheduler -l info

Console 3::

    # console 3
    $ cd examples/base
    $ python -m doctest tasks.py


>>> import json
>>> from sqlalchemy_celery_beat.models import PeriodicTask, IntervalSchedule
>>> from sqlalchemy_celery_beat.session import SessionManager

>>> beat_dburi = 'sqlite:///schedule.db'
>>> session_manager = SessionManager()
>>> engine, Session = session_manager.create_session(beat_dburi)
>>> session = Session()

# Disable 'echo-every-3-seconds' task
>>> task = session.query(PeriodicTask).filter_by(name='echo-every-3-seconds').first()
>>> task.enabled = False
>>> session.add(task)
>>> session.commit()


>>> schedule = session.query(IntervalSchedule).filter_by(every=10, period=IntervalSchedule.SECONDS).first()
>>> if not schedule:
...     schedule = IntervalSchedule(every=10, period=IntervalSchedule.SECONDS)
...     session.add(schedule)
...     session.commit()

# Add 'add-every-10s' task
>>> task = PeriodicTask(
...     interval=schedule,
...     name='add-every-10s',
...     task='tasks.add',  # name of task.
...     args=json.dumps([1, 5])
... )
>>> session.add(task)
>>> session.commit()
>>> print('Add ' + task.name)
Add add-every-10s

>>> task.args=json.dumps([10, 2])
>>> session.add(task)
>>> session.commit()
"""
import os
import time
import platform
import datetime as dt
from datetime import timedelta

from celery import Celery
from celery import schedules

from sqlalchemy_celery_beat.schedulers import DatabaseScheduler  # noqa
from sqlalchemy_celery_beat.clockedschedule import clocked

# load environment variable from .env
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path, override=True)

# for and convenient to test and modify
ECHO_EVERY_MINUTE = os.getenv('ECHO_EVERY_MINUTE', '0')
ECHO_EVERY_HOUR = os.getenv('ECHO_EVERY_HOUR', '8')

if platform.system() == 'Windows':
    # must set the environment variable in windows for celery,
    # or else celery maybe don't work
    os.environ['FORKED_BY_MULTIPROCESSING'] = '1'

# rabbitmq
backend = 'rpc://'
broker_url = 'amqp://guest:guest@127.0.0.1:5672//'


# this scheduler will be reset after the celery beat restart
beat_schedule = {
    'echo-every-3-seconds': {
        'task': 'tasks.echo',
        'schedule': timedelta(seconds=3),
        'args': ('hello', ),
        'options': {
            'expires': dt.datetime.utcnow() + timedelta(seconds=10)  # right
            # 'expires': dt.datetime.now() + timedelta(seconds=30)  # error
            # 'expires': 10  # right
        }
    },
    'add-every-minutes': {
        'task': 'tasks.add',
        'schedule': schedules.crontab('*', '*', '*'),
        'args': (1, 2)
    },
    'echo-every-hours': {
        'task': 'tasks.echo',
        'schedule': schedules.crontab(ECHO_EVERY_MINUTE, '*', '*'),
        'args': ('echo-every-hours',)
    },
    'echo-every-days': {
        'task': 'tasks.echo',
        'schedule': schedules.crontab(ECHO_EVERY_MINUTE, ECHO_EVERY_HOUR, '*'),
        'args': ('echo-every-days',)
    },
    'echo-at-clock-time': {
        'task': 'tasks.echo',
        'schedule': clocked(dt.datetime.utcnow() + timedelta(minutes=5)),
        'args': ('hello from the clock', ),
        'options':{'one_off': True}
    },
}

beat_scheduler = 'sqlalchemy_celery_beat.schedulers:DatabaseScheduler'

beat_sync_every = 0

# The maximum number of seconds beat can sleep between checking the schedule.
# default: 0
beat_max_loop_interval = 10

# configure sqlalchemy_celery_beat database uri
beat_dburi = 'sqlite:///schedule.db'
# beat_dburi = 'mysql+mysqlconnector://root:root@127.0.0.1/celery-schedule'

timezone = 'UTC'

# prevent memory leaks
worker_max_tasks_per_child = 10

celery = Celery('tasks',
                backend=backend,
                broker=broker_url)

config = {
    'beat_schedule': beat_schedule,
    # 'beat_scheduler': beat_scheduler,  # The command line parameters are configured, so there is no need to write them in the code here
    'beat_max_loop_interval': beat_max_loop_interval,
    'beat_dburi': beat_dburi,
    # 'beat_schema': 'celery',  # set this to none if you are using sqlite or you want all tables under default schema

    'timezone': timezone,
    'worker_max_tasks_per_child': worker_max_tasks_per_child
}

celery.conf.update(config)


@celery.task
def add(x, y):
    return x + y


@celery.task
def echo(data):
    print(data)


if __name__ == "__main__":
    celery.start()
    # import doctest
    # doctest.testmod()
