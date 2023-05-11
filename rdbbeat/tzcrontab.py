import datetime as dt
from collections import namedtuple
from datetime import datetime

import pytz
from celery import Celery, schedules
from sqlalchemy_utils import TimezoneType

schedstate = namedtuple("schedstate", ("is_due", "next"))


class TzAwareCrontab(schedules.crontab):
    """Timezone Aware Crontab."""

    def __init__(
        self,
        minute: str = "*",
        hour: str = "*",
        day_of_week: str = "*",
        day_of_month: str = "*",
        month_of_year: str = "*",
        tz: TimezoneType = pytz.utc,
        app: Celery = None,
    ) -> None:
        """Overwrite Crontab constructor to include a timezone argument."""
        self.tz = tz

        nowfun = self.nowfunc

        super(TzAwareCrontab, self).__init__(
            minute=minute,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
            # tz=tz,
            nowfun=nowfun,
            app=app,
        )

    def nowfunc(self) -> datetime:
        return self.tz.normalize(pytz.utc.localize(dt.datetime.utcnow()))

    def is_due(self, last_run_at: datetime) -> schedstate:
        """Calculate when the next run will take place.

        Return tuple of (is_due, next_time_to_check).
        The last_run_at argument needs to be timezone aware.

        """
        # convert last_run_at to the schedule timezone
        last_run_at = last_run_at.astimezone(self.tz)

        rem_delta = self.remaining_estimate(last_run_at)
        rem = max(rem_delta.total_seconds(), 0)
        due = rem == 0
        if due:
            rem_delta = self.remaining_estimate(self.now())
            rem = max(rem_delta.total_seconds(), 0)
        return schedstate(due, rem)

    # Needed to support pickling
    def __repr__(self) -> str:
        return (
            f"<crontab: {self._orig_minute} {self._orig_hour} "
            f"{self._orig_day_of_week} {self._orig_day_of_month} "
            f"{self._orig_month_of_year} (m/h/d/dM/MY), {self.tz}>"
        )

    def __reduce__(self) -> schedules.crontab:
        return (
            self.__class__,
            (
                self._orig_minute,
                self._orig_hour,
                self._orig_day_of_week,
                self._orig_day_of_month,
                self._orig_month_of_year,
                self.tz,
            ),
            None,
        )

    def __eq__(self, other: schedules.crontab) -> bool:
        if isinstance(other, schedules.crontab):
            return (
                other.month_of_year == self.month_of_year
                and other.day_of_month == self.day_of_month
                and other.day_of_week == self.day_of_week
                and other.hour == self.hour
                and other.minute == self.minute
                and other.tz == self.tz
            )
        raise NotImplementedError
