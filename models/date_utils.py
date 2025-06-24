"""
Date range helper functions
"""
from datetime import datetime
from typing import List


def date_in_range(date: datetime, from_date: datetime, to_date: datetime) -> bool:
    return from_date <= date <= to_date if date else False

def dates_in_range(dates: List[datetime], from_date: datetime, to_date: datetime) -> bool:
    return any(from_date <= date <= to_date for date in dates if date)
