from typing import Any, Dict, List
from datetime import datetime, timezone

from external.trino.client import TrinoClient

from models.date_utils import get_tomorrow_date, get_last_week_date, date_in_range
from models.queuer.input import QueuerInput
from models.queuer.params_strategy import params_method

from sources.jira.utils import JiraUtils


@params_method("jira")
def extract_jira_params(input: QueuerInput) -> Dict[str, Any]:
    """Returns the parameters needed for the jira queuer."""

    # Default dates: END=today - START=today-1 week
    date_end = input.date_end or get_tomorrow_date()  # date + 1 day
    date_start = input.date_start or get_last_week_date()  # date - 8 days

    # Filter issues that were updated in the given date range
    issues: List[str] = []
    start = datetime.strptime(date_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(date_end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    for issue in TrinoClient.get_issues_ids_and_last_update():
        last_updated = issue["last_updated"]
        if not isinstance(last_updated, datetime):
            last_updated = JiraUtils.parse_jira_datetime(last_updated)
        if date_in_range(last_updated, start, end):
            issues.append(issue["id"])

    return {
        "source_kind_id": "jira",
        "date_start": date_start,
        "date_end": date_end,
        "issues": issues,
    }
