import json
from typing import Dict

import pytest
from mock import patch
from sqlalchemy.orm.exc import NoResultFound

from rdbbeat.controller import (
    delete_task,
    get_crontab_schedule,
    is_crontab_used,
    schedule_task,
    update_task,
    update_task_enabled_status,
)
from rdbbeat.data_models import Schedule, ScheduledTask
from rdbbeat.db.models import CrontabSchedule, PeriodicTask
from rdbbeat.exceptions import PeriodicTaskNotFound


def test_get_new_crontab_schedule(scheduled_task):
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.query(CrontabSchedule).filter().one_or_none.return_value = None
        crontab = get_crontab_schedule(mock_session, Schedule(**scheduled_task.get("schedule")))
        assert crontab.minute == scheduled_task.get("schedule")["minute"]
        assert crontab.hour == scheduled_task.get("schedule")["hour"]
        assert crontab.day_of_week == scheduled_task.get("schedule")["day_of_week"]
        assert crontab.day_of_month == scheduled_task.get("schedule")["day_of_month"]
        assert crontab.month_of_year == scheduled_task.get("schedule")["month_of_year"]
        assert crontab.timezone == scheduled_task.get("schedule")["timezone"]


def test_get_existing_crontab_schedule(scheduled_task, scheduled_task_db_object):
    existing_crontab = scheduled_task_db_object.crontab
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.query(CrontabSchedule).filter().one_or_none.return_value = existing_crontab
        crontab = get_crontab_schedule(mock_session, Schedule(**scheduled_task.get("schedule")))
        assert existing_crontab == crontab


def test_schedule_task(scheduled_task_db_object, scheduled_task):
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.add.return_value = None
        mock_session.query(CrontabSchedule).filter().one_or_none.return_value = None

        actual_scheduled_task = schedule_task(mock_session, ScheduledTask.parse_obj(scheduled_task))

        expected_scheduled_task = scheduled_task_db_object

        assert actual_scheduled_task.name == expected_scheduled_task.name
        assert actual_scheduled_task.task == expected_scheduled_task.task
        assert actual_scheduled_task.schedule == expected_scheduled_task.schedule


def test_schedule_task_kwargs(scheduled_task_db_object, scheduled_task):
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.add.return_value = None
        mock_session.query(CrontabSchedule).filter().one_or_none.return_value = None

        actual_scheduled_task = schedule_task(
            mock_session, ScheduledTask.parse_obj(scheduled_task), report_metadata_uid="some_uid"
        )

        expected_scheduled_task = scheduled_task_db_object

        assert actual_scheduled_task.name == expected_scheduled_task.name
        assert actual_scheduled_task.task == expected_scheduled_task.task
        assert actual_scheduled_task.schedule == expected_scheduled_task.schedule
        assert actual_scheduled_task.kwargs == json.dumps({"report_metadata_uid": "some_uid"})


def test_update_task_enabled_status(scheduled_task_db_object):
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.query(PeriodicTask).get.return_value = scheduled_task_db_object

        periodic_task_id = 1
        updated_task = update_task_enabled_status(mock_session, False, periodic_task_id)

        assert updated_task.enabled is False


def test_update_task_enabled_status_fail():
    with patch("sqlalchemy.orm.Session") as mock_session:
        with pytest.raises(PeriodicTaskNotFound):
            mock_session.query(PeriodicTask).get.side_effect = NoResultFound()

            periodic_task_id = -1
            update_task_enabled_status(mock_session, False, periodic_task_id)


def test_update_task(scheduled_task_db_object):
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.query(PeriodicTask).get.return_value = scheduled_task_db_object
        mock_session.query(CrontabSchedule).filter().one_or_none.return_value = None

        new_schedule: Dict = {
            "minute": "24",
            "hour": "01",
            "day_of_week": "3",
            "day_of_month": "24",
            "month_of_year": "11",
            "timezone": "UTC",
        }
        new_scheduled_task: Dict = {
            "name": "task_2",
            "task": "echo2",
            "schedule": new_schedule,
        }
        new_schedule = CrontabSchedule(**new_schedule)

        expected_updated_task = PeriodicTask(
            crontab=new_schedule,
            name=new_scheduled_task["name"],
            task=new_scheduled_task["task"],
        )

        periodic_task_id = 1
        actual_updated_db_task = update_task(
            mock_session, ScheduledTask.parse_obj(new_scheduled_task), periodic_task_id
        )

        assert mock_session.query(PeriodicTask).get.call_count == 1
        assert actual_updated_db_task.name == expected_updated_task.name
        assert actual_updated_db_task.task == expected_updated_task.task
        assert actual_updated_db_task.schedule == expected_updated_task.schedule


def test_delete_task(scheduled_task_db_object):
    with patch("sqlalchemy.orm.Session") as mock_session:
        # Set up the mock_session
        periodic_task_id = 1
        mock_session.query(PeriodicTask).get.return_value = scheduled_task_db_object
        mock_session.delete.return_value = None
        # Delete task
        with patch("uxi_celery_scheduler.controller.is_crontab_used") as is_crontab_used:
            is_crontab_used.return_value = False
            actual_deleted_task = delete_task(mock_session, periodic_task_id)

        expected_deleted_task = scheduled_task_db_object

        assert actual_deleted_task.name == expected_deleted_task.name
        assert actual_deleted_task.task == expected_deleted_task.task
        assert actual_deleted_task.schedule == expected_deleted_task.schedule


def test_is_crontab_used(scheduled_task_db_object):
    with patch("sqlalchemy.orm.Session") as mock_session:
        mock_session.query(PeriodicTask).filter_by().all.return_value = None

        result = is_crontab_used(mock_session, scheduled_task_db_object.schedule)
        assert result is False
