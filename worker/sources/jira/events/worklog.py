from typing import List, Dict, Any
from datetime import datetime

from models.logger import logger
from models.date_utils import date_in_range, to_utc

from sources.jira.utils import JiraUtils
from sources.jira.query import JiraQuery


def extract_worklogs(
    issue_props: Dict[str, Any],
    issue_fields: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
    assignees_over_time: List[Dict[str, Any]] = [],
) -> List[Dict[str, Any]]:
    logger.info(f"Extracting worklogs for issue {issue_props['id']}")

    worklogs = issue_fields.get("worklog", {}).get("worklogs", [])
    if not worklogs:
        logger.info(f"No worklogs found for issue {issue_props['id']}")
        return []

    events = []
    for worklog in worklogs:
        events.extend(
            extract_worklog_events(
                worklog, issue_props, start_date, end_date, assignees_over_time
            )
        )

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_worklog_events(
    worklog: Dict[str, Any],
    issue_props: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
    assignees_over_time: List[Dict[str, Any]] = [],
) -> List[Dict[str, Any]]:
    events = []
    if not worklog:
        return events

    parent_item_id = issue_props["id"]
    event_props = extract_worklog_event_props(issue_props, worklog)

    created = JiraUtils.parse_jira_datetime(worklog["created"])
    employee_id = find_assignee_at(assignees_over_time, created)
    if not employee_id:
        employee_id = issue_props["reporter"]

    if (
        created
        and date_in_range(created, start_date, end_date)
        and not JiraUtils.is_system_account_id(employee_id)
    ):
        event_id = f"wl-{worklog['id']}-c"
        timezone = worklog["author"].get("timeZone", "UTC")
        event_time = created.replace(tzinfo=None).isoformat()
        event_time_utc = to_utc(created).replace(tzinfo=None).isoformat()
        events.append(
            {
                "source_kind_id": "jira",
                "parent_item_id": parent_item_id,
                "event_id": event_id,
                "event_type": "worklog_created",
                "relation_type": "author",
                "employee_id": employee_id,
                "event_time": event_time,
                "event_time_utc": event_time_utc,
                "timezone": timezone,
                "event_properties": event_props,
            }
        )

    updated = JiraUtils.parse_jira_datetime(worklog["updated"])
    update_employee_id = find_assignee_at(assignees_over_time, updated)
    if not update_employee_id:
        update_employee_id = issue_props["reporter"]

    if (
        updated
        and updated != created
        and date_in_range(updated, start_date, end_date)
        and not JiraUtils.is_system_account_id(update_employee_id)
    ):
        version = extract_worklog_version(worklog)
        event_id = f"wl-{worklog['id']}-u{version}"
        timezone = worklog["updateAuthor"].get("timeZone", "UTC")
        event_time = updated.replace(tzinfo=None).isoformat()
        event_time_utc = to_utc(updated).replace(tzinfo=None).isoformat()
        events.append(
            {
                "source_kind_id": "jira",
                "parent_item_id": parent_item_id,
                "event_id": event_id,
                "event_type": "worklog_updated",
                "relation_type": "author",
                "employee_id": update_employee_id,
                "event_time": event_time,
                "event_time_utc": event_time_utc,
                "timezone": timezone,
                "event_properties": event_props,
            }
        )

    return events


def extract_worklog_event_props(issue_props: dict, worklog: dict) -> dict:
    started = JiraUtils.parse_jira_datetime(worklog["started"])
    return {
        "issue_id": issue_props["id"],
        "project": issue_props["project"],
        "id": int(worklog["id"]),
        "url": worklog["self"],
        "started": started.isoformat(),
        "timeSpentSeconds": int(worklog["timeSpentSeconds"]),
    }


def extract_worklog_version(worklog: dict) -> int:
    """Extract plain text from Jira worklog body structure.

    Returns:
        tuple: (text_content, version) where text_content is the extracted plain text
               and version is the document version number
    """
    if not worklog.get("comment"):
        return 0
    return worklog["comment"].get("version", 1)


def find_assignee_at(assignees: List[Dict], at: datetime) -> str | None:
    if not assignees:
        return None
    for entry in assignees:
        if entry["since"] <= at:
            return entry["id"]
    return assignees[-1]["id"]
