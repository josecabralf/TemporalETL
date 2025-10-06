from datetime import datetime, timedelta


def get_tomorrow_date() -> str:
    """Returns the default end date as the tomorrow's date in YYYY-MM-DD format."""
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def get_last_week_date() -> str:
    """Returns the default start date as 8 days before today in YYYY-MM-DD format."""
    return (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")


def date_in_range(date: datetime, from_date: datetime, to_date: datetime) -> bool:
    return from_date <= date <= to_date if date else False


_week_days = {
    0: "Sunday",
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
}


def get_week_day(day_index: int) -> str:
    return _week_days.get(day_index, "Invalid day index")
