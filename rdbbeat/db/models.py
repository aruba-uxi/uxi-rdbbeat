# Copyright (c) 2018 AngelLiang
# Copyright (c) 2023 Hewlett Packard Enterprise Development LP
# MIT License

import datetime as dt
import logging
from typing import Any, Dict, Union

import pytz
import sqlalchemy as sa
from celery import schedules
from sqlalchemy import MetaData, func
from sqlalchemy.engine import Engine
from sqlalchemy.event import listen
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, class_mapper, foreign, relationship, remote
from sqlalchemy.sql import insert, select, update

from rdbbeat.tzcrontab import TzAwareCrontab

logger = logging.getLogger(__name__)

Base: Any = declarative_base(metadata=MetaData(schema="scheduler"))


def cronexp(field: str) -> str:
    """Representation of cron expression."""
    return field and str(field).replace(" ", "") or "*"


class ModelMixin:
    @classmethod
    def create(cls, **kw: Dict) -> "ModelMixin":
        return cls(**kw)

    def update(self, **kw: Dict) -> "ModelMixin":
        for attr, value in kw.items():
            setattr(self, attr, value)
        return self


class CrontabSchedule(Base, ModelMixin):
    __tablename__ = "celery_crontab_schedule"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    minute = sa.Column(sa.String(60 * 4), default="*")
    hour = sa.Column(sa.String(24 * 4), default="*")
    day_of_week = sa.Column(sa.String(64), default="*")
    day_of_month = sa.Column(sa.String(31 * 4), default="*")
    month_of_year = sa.Column(sa.String(64), default="*")
    timezone = sa.Column(sa.String(64), default="UTC")

    @property
    def schedule(self) -> TzAwareCrontab:
        return TzAwareCrontab(
            minute=self.minute,
            hour=self.hour,
            day_of_week=self.day_of_week,
            day_of_month=self.day_of_month,
            month_of_year=self.month_of_year,
            tz=pytz.timezone(self.timezone),
        )

    @classmethod
    def from_schedule(cls, session: Session, schedule: schedules.crontab) -> "CrontabSchedule":
        spec = {
            "minute": schedule._orig_minute,
            "hour": schedule._orig_hour,
            "day_of_week": schedule._orig_day_of_week,
            "day_of_month": schedule._orig_day_of_month,
            "month_of_year": schedule._orig_month_of_year,
        }
        if schedule.tz:
            spec.update({"timezone": schedule.tz.zone})
        model = session.query(CrontabSchedule).filter_by(**spec).first()
        if not model:
            model = cls(**spec)
            session.add(model)
            session.commit()

        return model


class PeriodicTaskChanged(Base, ModelMixin):
    """Helper table for tracking updates to periodic tasks."""

    __tablename__ = "celery_periodic_task_changed"

    id = sa.Column(sa.Integer, primary_key=True)
    last_update = sa.Column(sa.DateTime(timezone=True), nullable=False, default=dt.datetime.now)

    @classmethod
    def changed(
        cls, mapper: class_mapper, connection: Engine.connect, target: "PeriodicTask"
    ) -> None:
        """
        :param mapper: the Mapper which is the target of this event
        :param connection: the Connection being used
        :param target: the mapped instance being persisted
        """
        if not target.no_changes:
            cls.update_changed(mapper, connection, target)

    @classmethod
    def update_changed(
        cls, mapper: class_mapper, connection: Engine.connect, target: "PeriodicTask"
    ) -> None:
        """
        :param mapper: the Mapper which is the target of this event
        :param connection: the Connection being used
        :param target: the mapped instance being persisted
        """
        s = connection.execute(
            select([PeriodicTaskChanged]).where(PeriodicTaskChanged.id == 1).limit(1)
        )
        if not s:
            s = connection.execute(insert(PeriodicTaskChanged), last_update=dt.datetime.now())
        else:
            s = connection.execute(
                update(PeriodicTaskChanged)
                .where(PeriodicTaskChanged.id == 1)
                .values(last_update=dt.datetime.now())
            )

    @classmethod
    def last_change(cls, session: Session) -> Union[dt.datetime, None]:
        periodic_tasks = session.query(PeriodicTaskChanged).get(1)
        if periodic_tasks:
            return periodic_tasks.last_update
        return None


class PeriodicTask(Base, ModelMixin):
    __tablename__ = "celery_periodic_task"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    # name
    name = sa.Column(sa.String(255), unique=True)
    # task name
    task = sa.Column(sa.String(255))

    crontab_id = sa.Column(sa.Integer)
    crontab = relationship(
        CrontabSchedule,
        uselist=False,
        primaryjoin=foreign(crontab_id) == remote(CrontabSchedule.id),
    )

    args = sa.Column(sa.Text(), default="[]")
    kwargs = sa.Column(sa.Text(), default="{}")
    # queue for celery
    queue = sa.Column(sa.String(255))
    # exchange for celery
    exchange = sa.Column(sa.String(255))
    # routing_key for celery
    routing_key = sa.Column(sa.String(255))
    priority = sa.Column(sa.Integer())
    expires = sa.Column(sa.DateTime(timezone=True))

    # Execute only once
    one_off = sa.Column(sa.Boolean(), default=False)
    start_time = sa.Column(sa.DateTime(timezone=True))
    enabled = sa.Column(sa.Boolean(), default=True)
    last_run_at = sa.Column(sa.DateTime(timezone=True))
    total_run_count = sa.Column(sa.Integer(), nullable=False, default=0)
    # Change the time
    date_changed = sa.Column(sa.DateTime(timezone=True), default=func.now(), onupdate=func.now())
    description = sa.Column(sa.Text(), default="")

    no_changes = False

    @property
    def task_name(self) -> str:
        return self.task

    @task_name.setter
    def task_name(self, value: str) -> None:
        self.task = value

    @property
    def schedule(self) -> schedules.schedule:
        if self.crontab:
            return self.crontab.schedule
        raise ValueError(f"{self.name} schedule is None!")


listen(PeriodicTask, "after_insert", PeriodicTaskChanged.update_changed)
listen(PeriodicTask, "after_delete", PeriodicTaskChanged.update_changed)
listen(PeriodicTask, "after_update", PeriodicTaskChanged.changed)
listen(CrontabSchedule, "after_insert", PeriodicTaskChanged.update_changed)
listen(CrontabSchedule, "after_delete", PeriodicTaskChanged.update_changed)
listen(CrontabSchedule, "after_update", PeriodicTaskChanged.update_changed)
