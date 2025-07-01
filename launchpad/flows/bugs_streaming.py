from typing import List, Dict, Any, Iterator
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
async def extract_data_streaming(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    """
    Streaming version of bug data extraction that processes data in batches.
    This reduces memory usage for large datasets.
    """
    logger.info("Starting streaming extraction of Launchpad bug data for member: %s", query.member)
    lp = Launchpad.login_anonymously(
        consumer_name=query.application_name, 
        service_root=query.service_root, 
        version=query.version
    )
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")
    
    try:
        person = lp.people[query.member] # type: ignore
    except KeyError:
        logger.warning(f"Member {query.member} not found")
        return []
    except Exception as e:
        raise ValueError(f"Error fetching member {query.member}: {e}")

    if not person: 
        return []
    
    bug_tasks = person.searchTasks(
        created_since=query.data_date_start, 
        created_before=query.data_date_end, 
        status=bug_task_status
    )
    
    if not bug_tasks: 
        logger.info("No bug tasks found for member %s in the specified date range", query.member)
        return []
    
    person_link = person.self_link 
    time_zone = person.time_zone

    logger.info("Found %d bug tasks for member %s", len(bug_tasks), query.member)
    
    # Process bugs in streaming batches
    all_events = []
    batch_count = 0
    
    for event_batch in process_bug_batch_streaming(
        bug_tasks, person_link, time_zone, query.member, batch_size=50
    ):
        all_events.extend(event_batch)
        batch_count += 1
        
        # Log progress for large datasets
        if batch_count % 10 == 0:
            logger.info(f"Processed {batch_count} batches, {len(all_events)} events so far")
    
    logger.info(f"Streaming extraction complete: {len(all_events)} total events from {batch_count} batches")
    return all_events


def process_bug_batch_streaming(bug_tasks: List, person_link: str, time_zone: str, 
                               member: str, batch_size: int = 50) -> Iterator[List[Dict[str, Any]]]:
    """
    Process bugs in batches to reduce memory usage and enable streaming.
    
    Args:
        bug_tasks: List of bug tasks from Launchpad
        person_link: Person's Launchpad link
        time_zone: Person's timezone
        member: Member identifier
        batch_size: Number of bugs to process per batch
        
    Yields:
        Lists of event dictionaries for each batch
    """
    already_seen = set()  # To avoid duplicates
    
    for i in range(0, len(bug_tasks), batch_size):
        batch = bug_tasks[i:i + batch_size]
        events_batch = []
        
        logger.info(f"Processing bug batch {i//batch_size + 1}: bugs {i+1} to {min(i+batch_size, len(bug_tasks))} of {len(bug_tasks)}")
        
        for task in batch:
            bug = task.bug
            if bug.id in already_seen: 
                continue
            already_seen.add(bug.id)

            # Parent item data
            parent_item_id = f"b-{bug.id}"
            event_properties = extract_bug_event_props(bug, task)
            metrics = extract_bug_metrics(bug)

            # Process activities
            activity_count = len(bug.activity_collection) if hasattr(bug, 'activity_collection') else 0
            message_count = len(bug.messages) if hasattr(bug, 'messages') else 0
            
            logger.debug(f"Processing bug {bug.id} with {activity_count} activities and {message_count} messages")
            
            if hasattr(bug, 'activity_collection'):
                for idx, activity in enumerate(bug.activity_collection):
                    if activity.person_link != person_link:
                        continue
                    
                    events_batch.append({
                        'parent_item_id':       parent_item_id,
                        'event_id':             f"{parent_item_id}-a{idx}",
                        'relation_type':        'bug_activity',
                        'employee_id':          member,
                        'event_time_utc':       activity.datechanged.isoformat(),
                        'time_zone':            time_zone,
                        'relation_properties':  extract_activity_relation_props(activity),
                        'event_properties':     event_properties,
                        'metrics':              metrics
                    })

            # Process messages
            if hasattr(bug, 'messages'):
                for idx, message in enumerate(bug.messages):
                    if message.owner_link != person_link:
                        continue
                    
                    events_batch.append({
                        'parent_item_id':       parent_item_id,
                        'event_id':             f"{parent_item_id}-m{idx}",
                        'relation_type':        'bug_message',
                        'employee_id':          member,
                        'event_time_utc':       message.date_created.isoformat(),
                        'time_zone':            time_zone,
                        'relation_properties':  extract_message_relation_props(message),
                        'event_properties':     event_properties,
                        'metrics':              metrics
                    })
        
        logger.info(f"Batch {i//batch_size + 1} produced {len(events_batch)} events")
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