from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from models.date_utils import date_in_range, to_utc
from models.logger import logger

from sources.jira.utils import JiraUtils
from sources.jira.query import JiraQuery


# Global data structure to track assignees over time
# Each entry: {"id": str, "since": datetime}
# Stored in descending order by 'since'
assignees_over_time = []


def extract_changelog(
    issue_props: Dict[str, Any],
    issue_changelog: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
) -> tuple[List[Dict], List[Dict]]:
    """
    Extract changelog events from a Jira issue's changelog data.
    Returns
        - List of extracted events
        - List of assignees over time
    """
    logger.info(f"Extracting changelog for issue {issue_props['id']}")

    histories = issue_changelog.get("histories")
    if not histories:
        logger.warning(f"No changelog found for issue {issue_props['id']}")
        return ([], [])

    last_author: Optional[str] = None
    last_created: Optional[datetime] = None

    # Initialize assignees over time
    update_assignees_over_time({"from": issue_props["assignee"]}, datetime.min)

    events = []
    logger.info(f"Processing {len(histories)} changelog histories")
    for history in reversed(histories):
        created = JiraUtils.parse_jira_datetime(history["created"])
        if not date_in_range(created, start_date, end_date):
            if events:
                break
            else:
                continue  # skip until we reach the date range

        author = history["author"].get("emailAddress")
        last_event = (
            events.pop()
            if events and is_burst(created, author, last_created, last_author)
            else None
        )

        events.extend(extract_history_events(history, issue_props, created, last_event))

        # Update last event tracking variables
        last_author, last_created = author, created

    return events, assignees_over_time


"""
Helper functions to extract properties from changelog history objects
"""


def extract_history_events(
    history: Dict[str, Any],
    issue_props: Dict[str, Any],
    created: datetime,
    last_event: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    events = []
    if not history:
        return events

    items = history.get("items", [])
    if not items:
        return events

    employee_id = history["author"].get("emailAddress")
    if not employee_id or JiraUtils.is_system_account_mail(employee_id):
        return events

    # carry forward last event in burst to append changes to it
    if last_event:
        events.append(last_event)

    parent_item_id = issue_props["id"]
    history_id = history["id"]
    timezone = history["author"].get("timeZone", "UTC")
    event_time = created.replace(tzinfo=None).isoformat()
    event_time_utc = to_utc(created).replace(tzinfo=None).isoformat()
    event_props = get_history_event_base_props(issue_props, history)

    changes = []
    for item in items:
        field = item["field"].lower()
        if not field in FOI_CONFIG:
            changes.append(get_change(field, item))
            continue

        is_assignee = field == "assignee"
        if is_assignee:
            update_assignees_over_time(item, created)

        config = FOI_CONFIG[field]
        event_type = config.type
        event_id = f"{history_id}-{config.suffix}"
        event_props["change"] = get_change(field, item, is_assignee)
        event = {
            "source_kind_id": "jira",
            "parent_item_id": parent_item_id,
            "event_id": event_id,
            "event_type": event_type,
            "relation_type": "author",
            "employee_id": employee_id,
            "event_time": event_time,
            "event_time_utc": event_time_utc,
            "timezone": timezone,
            "event_properties": event_props,
        }
        events.append(event)

    if not events:  # in a burst there will be an event already
        # create a single 'changelog' event for the history entry
        config = FOI_CONFIG["changelog"]
        event_type = config.type
        event_id = f"{history_id}-{config.suffix}"
        event_props["changes"] = changes
        event = {
            "source_kind_id": "jira",
            "parent_item_id": parent_item_id,
            "event_id": event_id,
            "event_type": event_type,
            "relation_type": "author",
            "employee_id": employee_id,
            "event_time": event_time,
            "event_time_utc": event_time_utc,
            "timezone": timezone,
            "event_properties": event_props,
        }
        events.append(event)
    elif changes:
        # append any non-foi changes to the last event so we don't lose them
        changes_key = (
            "changes"
            if events[-1]["event_type"] == "changelog"
            else "additional_changes"
        )
        existing = events[-1]["event_properties"].get(changes_key) or []
        events[-1]["event_properties"][changes_key] = existing + changes

        # Event is being updated, so ensure date = latest
        events[-1]["event_time"] = event_time
        events[-1]["event_time_utc"] = event_time_utc

    return events


def get_history_event_base_props(issue_props: dict, history: dict) -> dict:
    return {
        "project": issue_props["project"],
        "issue_id": issue_props["id"],
        "history_id": int(history["id"]),
    }


def get_change(field: str, item: dict, is_assignee: bool = False) -> dict:
    keys = ["from", "to"] if is_assignee else ["fromString", "toString"]
    return {
        "field": field,
        "from": item.get(keys[0]),
        "to": item.get(keys[1]),
    }


def is_burst(
    current_created: datetime,
    current_author: str,
    last_created: Optional[datetime],
    last_author: Optional[str],
) -> bool:
    """
    Check if the current event is part of a burst with the last event.

    A burst is defined as:
    Events by the same author created within 3 seconds of each other.
    """
    if last_created is None or last_author is None:
        return False
    time_diff = abs((current_created - last_created).total_seconds())
    return time_diff < 3 and current_author == last_author


def update_assignees_over_time(item: dict, when: datetime) -> None:
    global assignees_over_time

    if when.tzinfo is None:  # always set tzinfo
        when = when.replace(tzinfo=timezone.utc)

    if len(assignees_over_time) == 0:
        assignees_over_time.append({"id": item["from"], "since": when})
        return

    assignees_over_time[-1]["since"] = when
    assignees_over_time.append(
        {"id": item["from"], "since": datetime.min.replace(tzinfo=timezone.utc)}
    )


class FieldConfig:
    def __init__(self, event_type: str, id_suffix: str):
        self.type = event_type
        self.suffix = id_suffix


FOI_CONFIG: Dict[str, FieldConfig] = {
    "acceptance criteria": FieldConfig("acceptance_changed", "acceptance"),
    "assignee": FieldConfig("assignee_changed", "assignee"),
    "comment": FieldConfig("comment_deleted", "comment"),
    "description": FieldConfig("description_changed", "description"),
    "priority": FieldConfig("priority_changed", "priority"),
    "status": FieldConfig("status_changed", "status"),
    "story points": FieldConfig("story_points_changed", "points"),
    "summary": FieldConfig("summary_changed", "summary"),
    "changelog": FieldConfig("changelog", "changelog"),
}
