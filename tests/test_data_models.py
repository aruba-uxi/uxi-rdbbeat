import pytest
from pydantic import ValidationError

from rdbbeat.data_models import Schedule


def test_schedule_pass():
    schedule = {
        "minute": "23",
        "hour": "00",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    Schedule.parse_obj(schedule)


def test_schedule_invalid_minute_type():
    schedule = {
        "minute": "minute",
        "hour": "00",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    with pytest.raises(ValueError, match="Minute: 'minute' is not a valid int"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_minute():
    schedule = {
        "minute": "200",
        "hour": "00",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    with pytest.raises(ValidationError, match="Minute value must range between 0 and 59"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_hour_type():
    schedule = {
        "minute": "23",
        "hour": "h",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    with pytest.raises(ValueError, match="Hour: 'h' is not a valid int"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_hour():
    schedule = {
        "minute": "23",
        "hour": "0055",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    with pytest.raises(ValidationError, match="Hour value must range between 0 and 23"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_day_of_week_type():
    schedule = {
        "minute": "23",
        "hour": "00",
        "day_of_week": "day",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    with pytest.raises(ValueError, match="Day of week: 'day' is not a valid int"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_day_of_week():
    schedule = {
        "minute": "23",
        "hour": "0055",
        "day_of_week": "22",
        "day_of_month": "23",
        "month_of_year": "12",
    }
    with pytest.raises(ValidationError, match="Day of the week value must range between 0 and 6"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_day_of_month_type():
    schedule = {
        "minute": "23",
        "hour": "00",
        "day_of_week": "2",
        "day_of_month": "day",
        "month_of_year": "12",
    }
    with pytest.raises(ValueError, match="Day of month: 'day' is not a valid int"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_day_of_month():
    schedule = {
        "minute": "23",
        "hour": "0055",
        "day_of_week": "2",
        "day_of_month": "32",
        "month_of_year": "12",
    }
    with pytest.raises(ValidationError, match="Day of the month value must range between 1 and 31"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_month_type():
    schedule = {
        "minute": "23",
        "hour": "00",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "month",
    }
    with pytest.raises(ValueError, match="Month: 'month' is not a valid int"):
        Schedule.parse_obj(schedule)


def test_schedule_invalid_month():
    schedule = {
        "minute": "23",
        "hour": "0055",
        "day_of_week": "2",
        "day_of_month": "23",
        "month_of_year": "0",
    }
    with pytest.raises(ValidationError, match="Month of year value must range between 0 and 12"):
        Schedule.parse_obj(schedule)
