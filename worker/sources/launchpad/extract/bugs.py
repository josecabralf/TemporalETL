from typing import Any, Dict, List

from models.etl.extract_strategy import extract_method
from models.logger import logger

from sources.launchpad.query import LaunchpadQuery
from sources.launchpad.config import LaunchpadConfiguration
from sources.launchpad.person import Person, get_user

bug_task_status = [
    "New",
    "Incomplete",
    "Opinion",
    "Invalid",
    "Won't Fix",
    "Expired",
    "Confirmed",
    "Triaged",
    "In Progress",
    "Deferred",
    "Fix Committed",
    "Fix Released",
    "Does Not Exist",
]


@extract_method(name="launchpad-bugs")
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    if not query.member:
        logger.warning("No member specified in query")
        return []

    logger.info("Extracting Launchpad bug data for member: %s", query.member)
    lp = LaunchpadConfiguration.get_launchpad_instance()
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    lp_user = get_user(query.member, lp)
    if not lp_user:
        return []  # either malformed name or inexistent

    logger.info("Connected to Launchpad member: %s", query.member)
    bug_tasks: List[Dict[str, Any]] = lp_user.searchTasks(
        created_since=query.date_start,
        created_before=query.date_end,
        status=bug_task_status,
    ).entries
    logger.info("Found %d bug tasks for member %s", len(bug_tasks), query.member)
    if not bug_tasks:
        return []

    already_seen = set()  # To avoid duplicates
    events = []
    person = Person(query.member, lp_user.time_zone, lp_user.self_link)
    for task in bug_tasks:
        bug_id = task["bug_link"].split("/")[-1]
        if bug_id in already_seen:
            continue
        events.extend(extract_bug_events(person, task, lp.bugs[bug_id]))  # type: ignore
        already_seen.add(bug_id)

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_bug_events(
    person: Person, task: Dict[str, Any], bug
) -> List[Dict[str, Any]]:
    events_batch = []  # List to hold all events for this batch

    parent_item_id = f"b-{bug.id}"

    # Created event
    if hasattr(bug, "date_created") and bug.date_created:
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-c",
                "event_type": "bug_created",
                "relation_type": "owner",
                "employee_id": person.name,
                "event_time_utc": bug.date_created.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_created(bug, task),
                "metrics": extract_metrics(bug),
            }
        )

    # Process activities
    if hasattr(bug, "activity_collection"):
        for idx, activity in enumerate(bug.activity_collection):
            if activity.person_link != person.link:
                continue
            events_batch.append(
                {
                    "parent_item_id": parent_item_id,
                    "event_type": "bug_activity",
                    "event_id": f"{parent_item_id}-a{idx}",
                    "relation_type": "author",
                    "employee_id": person.name,
                    "event_time_utc": activity.datechanged.isoformat(),
                    "time_zone": person.timezone,
                    "event_properties": extract_activity(activity, bug.id),
                }
            )

    # Process messages
    if hasattr(bug, "messages"):
        for idx, message in enumerate(bug.messages):
            if message.owner_link != person.link:
                continue
            events_batch.append(
                {
                    "parent_item_id": parent_item_id,
                    "event_id": f"{parent_item_id}-m{idx}",
                    "event_type": "bug_message",
                    "relation_type": "author",
                    "employee_id": person.name,
                    "event_time_utc": message.date_created.isoformat(),
                    "time_zone": person.timezone,
                    "event_properties": extract_message(message, bug.id),
                }
            )

    logger.info(
        f"Extracted {len(events_batch)} events for bug {parent_item_id} ({person.name})"
    )
    return events_batch


def base_event_props(bug_id: str) -> dict:
    return {
        "bug_id": bug_id,
    }


def extract_created(bug, task: Dict[str, Any]) -> dict:
    return {
        **base_event_props(bug.id),
        "title": bug.title,
        "link": bug.web_link,
        "information_type": bug.information_type,
        "private": bug.private,
        "security_related": bug.security_related,
        "name": bug.name,
        "tags": bug.tags,
        "importance": task.get("importance"),
        "owner_link": task.get("owner_link"),
    }


def extract_metrics(bug) -> dict:
    return {
        "heat": bug.heat,
        "number_of_duplicates": bug.number_of_duplicates,
        "users_affected_count": bug.users_affected_count,
        "users_affected_count_with_dupes": bug.users_affected_count_with_dupes,
        "users_unaffected_count": bug.users_unaffected_count,
    }


def extract_activity(activity, bug_id) -> dict:
    return {
        **base_event_props(bug_id),
        "watch_changed": activity.whatchanged,
        "old_value": activity.oldvalue,
        "new_value": activity.newvalue,
        "message": activity.message,
    }


def extract_message(message, bug_id) -> dict:
    return {
        **base_event_props(bug_id),
        "link": message.web_link,
        "owner": message.owner_link,
        "content": message.content,
        "subject": message.subject,
    }
