from typing import Any, Dict, List

from launchpadlib.launchpad import Launchpad

from models.etl.extract_cmd import extract_method
from models.logger import logger

from sources.launchpad.query import LaunchpadQuery


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
    logger.info("Extracting Launchpad bug data for member: %s", query.member)
    lp = Launchpad.login_anonymously(
        consumer_name=query.application_name,
        service_root=query.service_root,
        version=query.version,
    )
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    try:
        person = lp.people[query.member]  # type: ignore
    except KeyError:
        return []  # Member does not exist, return empty list
    except Exception as e:
        raise ValueError("Error fetching member %s: %s", query.member, e)

    if not person:
        return []

    logger.info("Connected to Launchpad member: %s", person.name)
    bug_tasks = person.searchTasks(
        created_since=query.data_date_start,
        created_before=query.data_date_end,
        status=bug_task_status,
    )
    logger.info("Found %d bug tasks for member %s", len(bug_tasks), query.member)
    if not bug_tasks:
        return []

    already_seen = set()  # To avoid duplicates
    events = []
    for task in bug_tasks:
        if task.bug.id in already_seen:
            continue
        events.extend(extract_bug_events(person, task))
        already_seen.add(task.bug.id)

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_bug_events(person, task) -> List[Dict[str, Any]]:
    events_batch = []  # List to hold all events for this batch

    # Person data (freezed)
    person_id = person.id
    person_link = person.self_link
    time_zone = person.time_zone

    # Parent item data
    bug = task.bug
    parent_item_id = f"b-{bug.id}"
    event_properties = extract_bug_event_props(bug, task)
    metrics = extract_bug_metrics(bug)

    # Process activities
    if hasattr(bug, "activity_collection"):
        for idx, activity in enumerate(bug.activity_collection):
            if activity.person_link != person_link:
                continue
            events_batch.append(
                {
                    "parent_item_id": parent_item_id,
                    "event_id": f"{parent_item_id}-a{idx}",
                    "relation_type": "bug_activity",
                    "employee_id": person_id,
                    "event_time_utc": activity.datechanged.isoformat(),
                    "time_zone": time_zone,
                    "relation_properties": extract_activity_relation_props(activity),
                    "event_properties": event_properties,
                    "metrics": metrics,
                }
            )

    # Process messages
    if hasattr(bug, "messages"):
        for idx, message in enumerate(bug.messages):
            if message.owner_link != person_link:
                continue
            events_batch.append(
                {
                    "parent_item_id": parent_item_id,
                    "event_id": f"{parent_item_id}-m{idx}",
                    "relation_type": "bug_message",
                    "employee_id": person_id,
                    "event_time_utc": message.date_created.isoformat(),
                    "time_zone": time_zone,
                    "relation_properties": extract_message_relation_props(message),
                    "event_properties": event_properties,
                    "metrics": metrics,
                }
            )

    logger.info(
        f"Extracted {len(events_batch)} events for bug {parent_item_id} ({person.id})"
    )
    return events_batch


def extract_bug_event_props(bug, task) -> dict:
    return {
        "title": bug.title if hasattr(bug, "title") else None,
        "description": bug.description if hasattr(bug, "description") else None,
        "link": bug.web_link if hasattr(bug, "web_link") else None,
        "information_type": bug.information_type
        if hasattr(bug, "information_type")
        else None,
        "private": bug.private if hasattr(bug, "private") else None,
        "security_related": bug.security_related
        if hasattr(bug, "security_related")
        else None,
        "name": bug.name if hasattr(bug, "name") else None,
        "tags": bug.tags if hasattr(bug, "tags") and len(bug.tags) > 0 else None,
        "status": task.status if hasattr(task, "status") else None,
        "importance": task.importance if hasattr(task, "importance") else None,
        "is_complete": task.is_complete if hasattr(task, "is_complete") else None,
        "owner_link": task.owner_link if hasattr(task, "owner_link") else None,
    }


def extract_bug_metrics(bug) -> dict:
    return {
        "heat": bug.heat if hasattr(bug, "heat") else None,
        "message_count": bug.message_count if hasattr(bug, "message_count") else None,
        "number_of_duplicates": bug.number_of_duplicates
        if hasattr(bug, "number_of_duplicates")
        else None,
        "users_affected_count": bug.users_affected_count
        if hasattr(bug, "users_affected_count")
        else None,
        "users_affected_count_with_dupes": bug.users_affected_count_with_dupes
        if hasattr(bug, "users_affected_count_with_dupes")
        else None,
        "users_unaffected_count": bug.users_unaffected_count
        if hasattr(bug, "users_unaffected_count")
        else None,
    }


def extract_activity_relation_props(activity) -> dict:
    return {
        "watch_changed": activity.whatchanged
        if hasattr(activity, "whatchanged")
        else None,
        "old_value": activity.oldvalue if hasattr(activity, "oldvalue") else None,
        "new_value": activity.newvalue if hasattr(activity, "newvalue") else None,
        "message": activity.message if hasattr(activity, "message") else None,
    }


def extract_message_relation_props(message) -> dict:
    return {
        "link": message.web_link if hasattr(message, "web_link") else None,
        "owner": message.owner_link if hasattr(message, "owner_link") else None,
        "content": message.content if hasattr(message, "content") else None,
        "subject": message.subject if hasattr(message, "subject") else None,
    }
