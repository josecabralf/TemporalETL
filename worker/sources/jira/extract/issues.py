from datetime import datetime
import json
from typing import Any, Dict, List

from external.trino.client import TrinoClient

from models.date_utils import to_utc
from models.logger import logger
from models.etl.extract_strategy import extract_method

from sources.jira.query import JiraQuery
from sources.jira.events.changelog import extract_changelog
from sources.jira.events.comments import extract_comments
from sources.jira.events.issue_created import extract_issue_created
from sources.jira.events.worklog import extract_worklogs


@extract_method("jira-issues")
async def extract_data(query: JiraQuery) -> List[Dict[str, Any]]:
    """
    Extract created issues from Trino within the specified date range for a given project.
    """
    events = []
    start_date = to_utc(datetime.strptime(query.date_start, "%Y-%m-%d"))
    end_date = to_utc(datetime.strptime(query.date_end, "%Y-%m-%d"))

    issue = get_issue(query.issue_id)
    if not issue:
        return events
    logger.info(f"Extracting data for issue {issue['id']}")

    issue_props = {
        "id": str(issue["id"]),
        "url": issue["url"],
        "project": issue["fields"].get("project", {}).get("key"),
        "assignee": issue["fields"].get("assignee", {}).get("accountId"),
        "reporter": issue["fields"].get("reporter", {}).get("accountId"),
    }

    # Extract issue created event
    if created := extract_issue_created(
        issue_props=issue_props,
        issue_fields=issue["fields"],
        start_date=start_date,
        end_date=end_date,
    ):
        events.append(created)

    # Extract comments
    events.extend(
        extract_comments(
            issue_props=issue_props,
            issue_fields=issue["fields"],
            start_date=start_date,
            end_date=end_date,
        )
    )

    changelogs, assignees_over_time = extract_changelog(
        issue_props=issue_props,
        issue_changelog=issue["changelog"],
        start_date=start_date,
        end_date=end_date,
    )
    if changelogs:
        events.extend(changelogs)

    events.extend(
        extract_worklogs(
            issue_props=issue_props,
            issue_fields=issue["fields"],
            start_date=start_date,
            end_date=end_date,
            assignees_over_time=assignees_over_time,
        )
    )

    return events


def get_issue(issue_id: str) -> Dict[str, Any] | None:
    """
    Fetch a single issue by its ID if it has activity from the specified date.
    """
    try:
        issue = TrinoClient.get_issue(issue_id)
        issue["fields"] = json.loads(issue["fields"])
        issue["changelog"] = json.loads(issue["changelog"])
    except IndexError:
        logger.info(f"No issue found with ID {issue_id}")
        issue = None
    except json.JSONDecodeError:
        logger.info(f"Malformed issue contents {issue_id}")
        issue = None

    return issue
