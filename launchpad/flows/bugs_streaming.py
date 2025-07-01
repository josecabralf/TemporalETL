from typing import List, Dict, Any, AsyncIterator
import logging

from launchpad.query import LaunchpadQuery
from models.extract_cmd import extract_method
from launchpadlib.launchpad import Launchpad


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

@extract_method(name="launchpad-bugs-streaming")
async def extract_data_streaming(query: LaunchpadQuery, chunk_size: int) -> AsyncIterator[List[Dict[str, Any]]]:
    logger.info("Starting enhanced streaming extraction of Launchpad bug data for member: %s with chunk size: %d", query.member, chunk_size)
    lp = Launchpad.login_anonymously(consumer_name=query.application_name, service_root=query.service_root, version=query.version)
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")
    
    try:
        person = lp.people[query.member] # type: ignore
    except KeyError:
        logger.warning(f"Member {query.member} not found")
        return
    except Exception as e:
        raise ValueError(f"Error fetching member {query.member}: {e}")

    if not person: return
    
    bug_tasks = person.searchTasks(
        created_since=query.data_date_start, 
        created_before=query.data_date_end, 
        status=bug_task_status
    )
    if not bug_tasks: 
        logger.info("No bug tasks found for member %s in the specified date range", query.member)
        return
    
    person_link = person.self_link 
    time_zone = person.time_zone

    chunk_count = 0
    already_seen = set()  # To avoid duplicates
    logger.info("Found %d bug tasks for member %s, processing in chunks of %d", len(bug_tasks), query.member, chunk_size)
    for i in range(0, len(bug_tasks), chunk_size):
        logger.info(f"Processing chunk {chunk_count}: bugs {i+1} to {min(i+chunk_size, len(bug_tasks))} of {len(bug_tasks)}")
        batch = bug_tasks[i:i + chunk_size]
        events_batch = []
        chunk_count += 1
        
        for task in batch:
            bug = task.bug
            if bug.id in already_seen: continue

            # Parent item data
            parent_item_id = f"b-{bug.id}"
            event_properties = extract_bug_event_props(bug, task)
            metrics = extract_bug_metrics(bug)

            # Process activities
            if hasattr(bug, 'activity_collection'):
                for idx, activity in enumerate(bug.activity_collection):
                    if activity.person_link != person_link: continue
                    events_batch.append({
                        'parent_item_id':       parent_item_id,
                        'event_id':             f"{parent_item_id}-a{idx}",
                        'relation_type':        'bug_activity',
                        'employee_id':          query.member,
                        'event_time_utc':       activity.datechanged.isoformat(),
                        'time_zone':            time_zone,
                        'relation_properties':  extract_activity_relation_props(activity),
                        'event_properties':     event_properties,
                        'metrics':              metrics
                    })

            # Process messages
            if hasattr(bug, 'messages'):
                for idx, message in enumerate(bug.messages):
                    if message.owner_link != person_link: continue
                    events_batch.append({
                        'parent_item_id':       parent_item_id,
                        'event_id':             f"{parent_item_id}-m{idx}",
                        'relation_type':        'bug_message',
                        'employee_id':          query.member,
                        'event_time_utc':       message.date_created.isoformat(),
                        'time_zone':            time_zone,
                        'relation_properties':  extract_message_relation_props(message),
                        'event_properties':     event_properties,
                        'metrics':              metrics
                    })
        
            already_seen.add(bug.id)

        logger.info(f"Chunk {chunk_count} produced {len(events_batch)} events")
        # Yield the chunk immediately instead of accumulating
        yield events_batch
        

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