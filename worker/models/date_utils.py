from datetime import datetime, timedelta
from pytz import timezone
from typing import List


def date_in_range(date: datetime, from_date: datetime, to_date: datetime) -> bool:
    return from_date <= date <= to_date if date else False


def dates_in_range(
    dates: List[datetime], from_date: datetime, to_date: datetime
) -> bool:
    return any(from_date <= date <= to_date for date in dates if date)


def get_week_start_date(date_obj: datetime) -> str:
    """Calculate the Monday date for the week containing the given date.

    Args:
        date_obj: datetime object for which to find the week start

    Returns:
        ISO date string (YYYY-MM-DD) representing the Monday of the week
    """
    days_since_monday = date_obj.weekday()
    week_start = date_obj - timedelta(days=days_since_monday)
    return week_start.strftime("%Y-%m-%d")


def change_timezone(date_str: str, from_tz: str, to_tz: str) -> str:
    """Convert a date string from one timezone to another.

    Args:
        date_str: Date string in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        from_tz: Source timezone (e.g., 'UTC')
        to_tz: Target timezone (e.g., 'America/New_York')

    Returns:
        Converted date string in the target timezone
    """
    utc_dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone(from_tz))
    target_dt = utc_dt.astimezone(timezone(to_tz))
    return target_dt.isoformat()


def to_utc(date: datetime) -> datetime:
    """Convert a datetime object to UTC timezone.

    Args:
        date: datetime object to convert

    Returns:
        datetime object in UTC timezone
    """
    if date.tzinfo is None:
        # Assume the input date is in UTC
        return date.replace(tzinfo=timezone("UTC"))
    return date.astimezone(timezone("UTC"))
