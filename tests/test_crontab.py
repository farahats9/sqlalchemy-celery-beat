import pytest

from sqlalchemy_celery_beat.models import CrontabSchedule
from sqlalchemy_celery_beat.session import SessionManager, session_cleanup


class TestMixin:
    @pytest.fixture(autouse=True)
    def setup_scheduler(self, app):
        self.app = app
        self.app.conf.beat_dburi = 'sqlite:///tests/testing.db'
        Session = SessionManager()
        session = Session.session_factory(self.app.conf.beat_dburi)
        self.session = session

    def create_model(self, **kw):
        with session_cleanup(self.session):
            s = CrontabSchedule(**kw)
            self.session.add(s)
            self.session.commit()
            return s


class test_MinuteTests(TestMixin):

    def test_good(self):
        with session_cleanup(self.session):
            minutes = ('*', '0', '1', '54', '59', '1,2,59', '43,2', '5,20,25,43', '1-4', '1-29', '45-59', '*/4', '*/43', '1-2/43')
            for m in minutes:
                s = CrontabSchedule(minute=m)
                self.session.add(s)
            self.session.commit()

    def test_space(self):
        s = self.create_model(minute='1, 2')
        assert s.minute == '1,2'

    def test_big_number(self):
        with pytest.raises(ValueError):
            self.create_model(minute='60')
        with pytest.raises(ValueError):
            self.create_model(minute='420')
        with pytest.raises(ValueError):
            self.create_model(minute='100500')

    def test_text(self):
        with pytest.raises(ValueError):
            self.create_model(minute='fsd')
        with pytest.raises(ValueError):
            self.create_model(minute='.')
        with pytest.raises(ValueError):
            self.create_model(minute='432a')

    def test_out_range(self):
        with pytest.raises(ValueError):
            self.create_model(minute='0-432')
        with pytest.raises(ValueError):
            self.create_model(minute='342-432')
        with pytest.raises(ValueError):
            self.create_model(minute='4-60')

    # def test_bad_range(self):
    #     with pytest.raises(ValueError):
    #         self.create_model(minute='10-4')

    def test_bad_slice(self):
        # with pytest.raises(ValueError):
        #     self.create_model(minute='*/100')
        with pytest.raises(ValueError):
            self.create_model(minute='10/30')
        # with pytest.raises(ValueError):
        #     self.create_model(minute='10-20/100')


class test_HourTests(TestMixin):

    def test_good(self):
        with session_cleanup(self.session):
            hour = ('*', '0', '1', '22', '23', '1,2,23', '23,2', '5,20,21,22', '1-4', '1-23', '*/4', '*/22', '1-2/5')
            for h in hour:
                s = CrontabSchedule(hour=h)
                self.session.add(s)
            self.session.commit()

    def test_space(self):
        s = self.create_model(hour='1, 2')
        assert s.hour == '1,2'

    def test_big_number(self):
        with pytest.raises(ValueError):
            self.create_model(hour='24')
        with pytest.raises(ValueError):
            self.create_model(hour='420')
        with pytest.raises(ValueError):
            self.create_model(hour='100500')

    def test_text(self):
        with pytest.raises(ValueError):
            self.create_model(hour='fsd')
        with pytest.raises(ValueError):
            self.create_model(hour='.')
        with pytest.raises(ValueError):
            self.create_model(hour='432a')

    def test_out_range(self):
        with pytest.raises(ValueError):
            self.create_model(hour='0-24')
        with pytest.raises(ValueError):
            self.create_model(hour='342-432')
        with pytest.raises(ValueError):
            self.create_model(hour='4-25')

    # def test_bad_range(self):
    #     with pytest.raises(ValueError):
    #         self.create_model(hour='10-4')

    def test_bad_slice(self):
        # with pytest.raises(ValueError):
        #     self.create_model(hour='*/100')
        with pytest.raises(ValueError):
            self.create_model(hour='10/30')
        # with pytest.raises(ValueError):
        #     self.create_model(hour='10-20/100')


class test_DayOfMonthTests(TestMixin):
    def test_good(self):
        with session_cleanup(self.session):
            day_of_month = ('*', '1', '29', '31', '1,2,31', '30,2', '5,20,25,31', '1-4', '1-30', '*/4', '*/22', '1-2/5')
            for d in day_of_month:
                s = CrontabSchedule(day_of_month=d)
                self.session.add(s)
            self.session.commit()

    def test_space(self):
        s = self.create_model(day_of_month='1, 2')
        assert s.day_of_month == '1,2'

    def test_zero(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_month='0')

    def test_big_number(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_month='32')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='420')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='100500')

    def test_text(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_month='fsd')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='.')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='432a')

    def test_out_range(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_month='0-32')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='342-432')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='4-33')

    # def test_bad_range(self):
    #     with pytest.raises(ValueError):
    #         self.create_model(day_of_month='10-4')

    def test_bad_slice(self):
        # with pytest.raises(ValueError):
        #     self.create_model(day_of_month='*/100')
        with pytest.raises(ValueError):
            self.create_model(day_of_month='10/30')
        # with pytest.raises(ValueError):
        #     self.create_model(day_of_month='10-20/100')


class test_MonthTests(TestMixin):
    def test_good(self):
        with session_cleanup(self.session):
            month_of_year = ('*', '1', '10', '12', '1,2,12', '12,2', '5,10,11,12', '1-4', '1-12', '*/4', '*/12', '1-2/12')
            for m in month_of_year:
                s = CrontabSchedule(month_of_year=m)
                self.session.add(s)
            self.session.commit()

    def test_good_month_name(self):
        """ celery crontab does not support this yet
        """
        with session_cleanup(self.session):
            month_of_year = ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec')
            for m in month_of_year:
                s = CrontabSchedule(month_of_year=m)
                self.session.add(s)
            self.session.commit()

    def test_good_month_name_case(self):
        """ celery crontab does not support this yet
        """
        with session_cleanup(self.session):
            month_of_year = ('jan', 'JAN', 'JaN')
            for m in month_of_year:
                s = CrontabSchedule(month_of_year=m)
                self.session.add(s)
            self.session.commit()

    def test_space(self):
        s = self.create_model(month_of_year='1, 2')
        assert s.month_of_year == '1,2'

    def test_zero(self):
        with pytest.raises(ValueError):
            self.create_model(month_of_year='0')

    def test_big_number(self):
        with pytest.raises(ValueError):
            self.create_model(month_of_year='13')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='420')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='100500')

    def test_text(self):
        with pytest.raises(ValueError):
            self.create_model(month_of_year='fsd')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='.')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='432a')

    def test_out_range(self):
        with pytest.raises(ValueError):
            self.create_model(month_of_year='0-13')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='342-432')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='4-14')

    # def test_bad_range(self):
    #     with pytest.raises(ValueError):
    #         self.create_model(month_of_year='10-4')

    def test_bad_slice(self):
        # with pytest.raises(ValueError):
        #     self.create_model(month_of_year='*/13')
        with pytest.raises(ValueError):
            self.create_model(month_of_year='10/30')
        # with pytest.raises(ValueError):
        #     self.create_model(month_of_year='10-20/100')


class test_DayOfWeekTests(TestMixin):
    def test_good(self):
        with session_cleanup(self.session):
            day_of_week = ('*', '1', '6', '1,2,6', '6,2', '5,6,4,6', '1-4', '1-6', '*/4', '*/6', '2-6/5')
            for d in day_of_week:
                s = CrontabSchedule(day_of_week=d)
                self.session.add(s)
            self.session.commit()

    def test_good_week_name(self):
        with session_cleanup(self.session):
            day_of_week = ('sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat')
            for d in day_of_week:
                s = CrontabSchedule(day_of_week=d)
                self.session.add(s)
            self.session.commit()

    def test_good_week_name_case(self):
        with session_cleanup(self.session):
            day_of_week = ('mon', 'MoN', 'MON')
            for d in day_of_week:
                s = CrontabSchedule(day_of_week=d)
                self.session.add(s)
            self.session.commit()

    def test_space(self):
        s = self.create_model(day_of_week='1, 2')
        assert s.day_of_week == '1,2'

    def test_big_number(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_week='8')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='420')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='100500')

    def test_text(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_week='fsd')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='.')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='432a')

    def test_out_range(self):
        with pytest.raises(ValueError):
            self.create_model(day_of_week='0-8')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='342-432')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='4-9')

    # def test_bad_range(self):
    #     with pytest.raises(ValueError):
    #         self.create_model(day_of_week='10-4')

    def test_bad_slice(self):
        # with pytest.raises(ValueError):
        #     self.create_model(day_of_week='*/8')
        with pytest.raises(ValueError):
            self.create_model(day_of_week='10/30')
        # with pytest.raises(ValueError):
        #     self.create_model(day_of_week='10-20/100')
