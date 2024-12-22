from datetime import datetime
from typing import List
from returns.result import Result, Success, Failure

from app.repository.mongo_repository import get_terror_events_by_time_range


def get_terror_list_by_date(date_start, date_type, time_range) -> Result[List, str]:
    if date_type not in ["year", "month", "day"]:
        return Failure("Invalid date type")

    try:
        date_start = datetime.strptime(date_start, "%Y-%m-%d")
    except ValueError:
        return Failure("Invalid date format")

    if date_type == "year":
        date_end = date_start.replace(year=date_start.year + time_range)
    elif date_type == "month":
        date_end = date_start.replace(month=date_start.month + time_range)
    else:
        date_end = date_start.replace(day=date_start.day + time_range)
    return get_terror_events_by_time_range(date_start, date_end)

