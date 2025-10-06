from typing import List, Dict, Any
from datetime import datetime

from models.logger import logger
from models.date_utils import date_in_range, to_utc

from sources.jira.utils import JiraUtils
from sources.jira.query import JiraQuery


def extract_comments(
    issue_props: Dict[str, Any],
    issue_fields: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    logger.info(f"Extracting comments for issue {issue_props['id']}")

    comments = issue_fields.get("comment", {}).get("comments", [])
    if not comments:
        logger.info(f"No comments found for issue {issue_props['id']}")
        return []

    events = []
    for comment in comments:
        events.extend(
            extract_comment_events(comment, issue_props, start_date, end_date)
        )

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_comment_events(
    comment: Dict[str, Any],
    issue_props: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    events = []
    if not comment:
        return events

    parent_item_id = issue_props["id"]
    event_props, version = extract_comment_event_props(issue_props, comment)

    created = JiraUtils.parse_jira_datetime(comment["created"])
    employee_id = comment["author"].get("emailAddress")
    if (
        created
        and date_in_range(created, start_date, end_date)
        and employee_id
        and not JiraUtils.is_system_account_mail(employee_id)
    ):
        event_id = f"c-{comment['id']}-c"
        timezone = comment["author"].get("timeZone", "UTC")
        event_time = created.replace(tzinfo=None).isoformat()
        event_time_utc = to_utc(created).replace(tzinfo=None).isoformat()
        events.append(
            {
                "source_kind_id": "jira",
                "parent_item_id": parent_item_id,
                "event_id": event_id,
                "event_type": "comment_created",
                "relation_type": "author",
                "employee_id": employee_id,
                "event_time": event_time,
                "event_time_utc": event_time_utc,
                "timezone": timezone,
                "event_properties": event_props,
            }
        )

    updated = JiraUtils.parse_jira_datetime(comment["updated"])
    update_employee_id = comment["updateAuthor"].get("emailAddress")
    if (
        updated
        and updated != created
        and date_in_range(updated, start_date, end_date)
        and update_employee_id
        and not JiraUtils.is_system_account_mail(update_employee_id)
    ):
        event_id = f"c-{comment['id']}-u{version}"
        timezone = comment["updateAuthor"].get("timeZone", "UTC")
        event_time = updated.replace(tzinfo=None).isoformat()
        event_time_utc = to_utc(updated).replace(tzinfo=None).isoformat()
        events.append(
            {
                "source_kind_id": "jira",
                "parent_item_id": parent_item_id,
                "event_id": event_id,
                "event_type": "comment_updated",
                "relation_type": "author",
                "employee_id": update_employee_id,
                "event_time": event_time,
                "event_time_utc": event_time_utc,
                "timezone": timezone,
                "event_properties": event_props,
            }
        )

    return events


def extract_comment_event_props(issue_props: dict, comment: dict) -> tuple[dict, int]:
    body = comment.get("body", {})
    version = body.get("version", 0)
    content, mentions = JiraUtils.parse_adf(body)
    props = {
        "comment_version": version,
        "issue_id": issue_props["id"],
        "project": issue_props["project"],
        "id": int(comment["id"]),
        "url": comment["self"],
        "content": content,
        "mentions": mentions,
    }

    return props, version


def extract_comment_version(comment: dict) -> int:
    """Extract plain text from sources.jira comment body structure.

    Returns:
        tuple: (text_content, version) where text_content is the extracted plain text
               and version is the document version number
    """
    if not comment.get("body"):
        return 0
    return comment["body"].get("version", 1)
