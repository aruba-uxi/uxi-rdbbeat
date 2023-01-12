import pytest

from rdbbeat.db.models import CrontabSchedule, PeriodicTask


@pytest.fixture
def scheduled_task():
    schedule = {
        "minute": "23",
        "hour": "00",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "12",
        "timezone": "UTC",
    }
    scheduled_task = {
        "name": "task_1",
        "task": "echo",
        "schedule": schedule,
    }

    return scheduled_task


@pytest.fixture
def scheduled_task_db_object(scheduled_task):
    task = PeriodicTask(
        crontab=CrontabSchedule(**scheduled_task["schedule"]),
        name=scheduled_task["name"],
        task=scheduled_task["task"],
    )

    return task
