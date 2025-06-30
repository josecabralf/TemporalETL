from typing import List, Dict, Any

from launchpad.query import LaunchpadQuery
from models.extract_cmd import extract_method

from launchpadlib.launchpad import Launchpad

import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    "Does Not Exist"
]

@extract_method.defn(name="launchpad-bugs")
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    logger.info("Extracting Launchpad bug data for member: %s", query.member)
    lp = Launchpad.login_anonymously(consumer_name=query.application_name, service_root=query.service_root, version=query.version)
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")
    
    try:
        person = lp.people[query.member] # type: ignore
    except KeyError:
        return []
    except Exception as e:
        raise ValueError(f"Error fetching member %s: %s", query.member, e)

    if not person: return []
    
    bug_tasks = person.searchTasks(
        created_since = query.data_date_start, created_before = query.data_date_end, 
        status = bug_task_status
    )
    if not bug_tasks: return []
    
    person_link = person.self_link 
    time_zone = person.time_zone

    events = []
    logger.info("Found %d bug tasks for member %s", len(bug_tasks), query.member)
    for task in bug_tasks:
        bug = task.bug
        parent_item_id = f"b-{bug.id}"

        event_properties = extract_bug_event_props(bug, task)
        metrics = extract_bug_metrics(bug)

        # Look for bug activities linked to member
        logger.info("Processing bug %s with %d activities and %d messages", bug.id, len(bug.activity_collection), len(bug.messages))
        for idx, activity in enumerate(bug.activity_collection):
            if activity.person_link != person_link:
                continue # If the member is not involved in the bug, we can skip it

            event_id = f"{parent_item_id}-a{idx}"
            event_time_utc = activity.datechanged.isoformat()
            relation_type = 'bug_activity'

            relation_properties = extract_activity_relation_props(activity)
            
            activity_info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          query.member,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              metrics
            }
            events.append(activity_info)

        # Look for bug messages linked to member
        for idx, message in enumerate(bug.messages):
            if message.owner_link != person_link:
                continue # If the member is not involved in the message, we can skip it

            event_id = f"{parent_item_id}-m{idx}"
            event_time_utc = message.date_created.isoformat()
            relation_type = 'bug_message'

            relation_properties = extract_message_relation_props(message)

            message_info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          query.member,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              metrics
            }
            events.append(message_info)

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""
def extract_bug_event_props(bug, task) -> dict:
    # Filter out None/empty values and create dictionary
    property_mappings = [
        (bug.title, 'title'),
        (bug.description, 'description'),
        (bug.web_link, 'link'),
        (bug.information_type, 'information_type'),
        (bug.private, 'private'),
        (bug.security_related, 'security_related'),
        (bug.name, 'name'),
        (bug.tags if len(bug.tags) > 0 else None, 'tags'),

        (task.status, 'status'),
        (task.importance, 'importance'),
        (task.is_complete, 'is_complete'),
        (task.owner_link, 'owner_link'),
    ]
    event_properties = {}
    event_properties.update({key: value for value, key in property_mappings if value})

    return event_properties


def extract_bug_metrics(bug) -> dict:
    # Filter out None/empty values and create dictionary
    property_mappings = [
        (bug.heat, 'heat'),
        (bug.message_count, 'message_count'),
        (bug.number_of_duplicates, 'number_of_duplicates'),
        (bug.users_affected_count, 'users_affected_count'),
        (bug.users_affected_count_with_dupes, 'users_affected_count_with_duplicates'),
        (bug.users_unaffected_count, 'users_unaffected_count'),
    ]
    metrics = {}
    metrics.update({key: value for value, key in property_mappings if value})

    return metrics


def extract_activity_relation_props(activity) -> dict:
    # Filter out None/empty values and create dictionary
    property_mappings = [
        (activity.whatchanged, 'whatchanged'),
        (activity.oldvalue, 'old_value'),
        (activity.newvalue, 'new_value'),
        (activity.message, 'message'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})

    return relation_properties


def extract_message_relation_props(message) -> dict:
    # Filter out None/empty values and create dictionary
    property_mappings = [
        (message.web_link, 'link'),
        (message.owner_link, 'owner'),
        (message.content, 'content'),
        (message.subject, 'subject'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})

    return relation_properties