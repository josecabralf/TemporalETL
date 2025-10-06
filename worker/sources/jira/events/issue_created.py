from datetime import datetime
from typing import Dict, Any

from models.logger import logger
from models.date_utils import to_utc, date_in_range

from sources.jira.utils import JiraUtils
from sources.jira.query import JiraQuery


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_issue_created(
    issue_props: dict,
    issue_fields: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
) -> Dict[str, Any] | None:
    if not issue_fields:
        return None

    employee_id = issue_fields["reporter"].get("emailAddress")
    if not employee_id or JiraUtils.is_system_account_mail(employee_id):
        return None

    created = JiraUtils.parse_jira_datetime(issue_fields["created"])
    if not created or not date_in_range(created, start_date, end_date):
        return None

    logger.info(f"Extracting issue created event for issue {issue_props.get('id')}")
    parent_id = issue_fields["parent"].get("id") if issue_fields.get("parent") else None
    event_id = f"i-{issue_props['id']}-c"
    event_time = created.replace(tzinfo=None).isoformat()
    event_time_utc = to_utc(created).replace(tzinfo=None).isoformat()
    timezone = issue_fields.get("reporter_tz", "UTC")
    event_props = extract_issue_event_props(issue_props, issue_fields)

    return {
        "source_kind_id": "jira",
        "parent_item_id": parent_id,
        "event_id": event_id,
        "event_type": "issue_created",
        "relation_type": "reporter",
        "employee_id": employee_id,
        "event_time": event_time,
        "event_time_utc": event_time_utc,
        "timezone": timezone,
        "event_properties": event_props,
    }


def extract_issue_event_props(issue_props: dict, issue_fields: dict) -> dict:
    project = issue_fields.get("project", {}).get("key")
    issue_type = issue_fields.get("issuetype", {}).get("name")
    description, mentions = JiraUtils.parse_adf(issue_fields.get("description", {}))
    return {
        "project": project,
        "issue_id": issue_props["id"],
        "issue_type": issue_type,
        "url": issue_props["url"],
        "summary": issue_fields["summary"],
        "description": description,
        "mentions": mentions,
    }
