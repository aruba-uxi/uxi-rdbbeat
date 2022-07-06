import json
from typing import Any

from sqlalchemy import and_
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from uxi_celery_scheduler.data_models import Schedule, ScheduledTask
from uxi_celery_scheduler.db.models import CrontabSchedule, PeriodicTask
from uxi_celery_scheduler.exceptions import PeriodicTaskNotFound


def get_crontab_schedule(session: Session, schedule: Schedule) -> CrontabSchedule:
    crontab = (
        session.query(CrontabSchedule)
        .filter(
            and_(
                CrontabSchedule.minute == schedule.minute,
                CrontabSchedule.hour == schedule.hour,
                CrontabSchedule.day_of_week == schedule.day_of_week,
                CrontabSchedule.day_of_month == schedule.day_of_month,
                CrontabSchedule.month_of_year == schedule.month_of_year,
                CrontabSchedule.timezone == schedule.timezone,
            )
        )
        .one_or_none()
    )
    return crontab or CrontabSchedule(**schedule.dict())


def schedule_task(
    session: Session,
    scheduled_task: ScheduledTask,
    **kwargs: Any,
) -> PeriodicTask:
    """
    Schedule a task by adding a periodic task entry.
    """
    crontab = get_crontab_schedule(session=session, schedule=scheduled_task.schedule)
    task = PeriodicTask(
        crontab=crontab,
        name=scheduled_task.name,
        task=scheduled_task.task,
        kwargs=json.dumps(kwargs),
    )
    session.add(task)

    return task


def update_task_enabled_status(
    session: Session,
    enabled_status: bool,
    periodic_task_id: int,
) -> PeriodicTask:
    """
    Update task enabled status (if task is enabled or disabled).
    """
    try:
        task = session.query(PeriodicTask).get(periodic_task_id)
        task.enabled = enabled_status
        session.add(task)

    except NoResultFound as e:
        raise PeriodicTaskNotFound() from e

    return task


def update_task(
    session: Session,
    scheduled_task: ScheduledTask,
    periodic_task_id: int,
) -> PeriodicTask:
    """
    Update the details of a task including the crontab schedule
    """
    try:
        task = session.query(PeriodicTask).get(periodic_task_id)

        task.crontab = get_crontab_schedule(session, scheduled_task.schedule)
        task.name = scheduled_task.name
        task.task = scheduled_task.task
        session.add(task)

    except NoResultFound as e:
        raise PeriodicTaskNotFound() from e

    return task


def crontab_is_used(session: Session, crontab_schedule: CrontabSchedule) -> bool:
    schedules = session.query(PeriodicTask).filter_by(crontab=crontab_schedule).all()
    return True if schedules else False


def delete_task(session: Session, periodic_task_id: int) -> PeriodicTask:
    try:
        task = session.query(PeriodicTask).get(periodic_task_id)
        session.delete(task)
        session.flush()
        if not crontab_is_used(session, task.crontab):
            session.delete(task.crontab)
        return task
    except NoResultFound as e:
        raise PeriodicTaskNotFound() from e
