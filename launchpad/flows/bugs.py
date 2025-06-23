from typing import List, Any
from launchpad.query import LaunchpadQuery
from models.etl_flow import ETLFlow
from models.event import Event
from temporalio import activity
from launchpadlib.launchpad import Launchpad


class BugsFlow(ETLFlow):
    """
    ETL workflow implementation for processing Launchpad bug data.
    """
    queue_name = "launchpad-bugs-task-queue"
    
    @staticmethod
    def get_activities() -> List[Any]:
        return [extract_data, transform_data, load_data]

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

@activity.defn
async def extract_data(query: LaunchpadQuery) -> List[dict]:
    lp = Launchpad.login_anonymously(query.application_name, query.service_root, query.version)
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")
    
    person = lp.people[query.member]
    if not person: return []
    
    bug_tasks = person.searchTasks(
        created_since = query.data_date_start, created_before = query.data_date_end, 
        status = bug_task_status
    )
    if not bug_tasks: return []
    
    person_link = person.self_link 
    time_zone = person.time_zone

    events = []
    for task in bug_tasks:
        bug = task.bug
        parent_item_id = str(bug.id)

        # Filter out None/empty values and create dictionary
        property_mappings = [
            (bug.title, 'title'),
            (bug.description, 'description'),
            (bug.web_link, 'link'),
            (bug.heat, 'heat'),
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

        # Look for bug activities linked to member
        for idx, activity in enumerate(bug.activity_collection):
            if activity.person_link != person_link:
                continue # If the member is not involved in the bug, we can skip it

            event_id = f"{bug.id}-activity-{idx}"
            event_time_utc = activity.datechanged.isoformat()
            relation_type = 'bug_activity'

            # Filter out None/empty values and create dictionary
            property_mappings = [
                (activity.whatchanged, 'whatchanged'),
                (activity.oldvalue, 'old_value'),
                (activity.newvalue, 'new_value'),
                (activity.message, 'message'),
            ]
            relation_properties = {}
            relation_properties.update({key: value for value, key in property_mappings if value})
            
            activity_info = {
                'parent_item_id': parent_item_id,
                'event_id': event_id,
                'relation_type': relation_type,
                'employee_id': query.member,
                'event_time_utc': event_time_utc,
                'time_zone': time_zone,
                'relation_properties': relation_properties,
                'event_properties': event_properties
            }
            events.append(activity_info)

        # Look for bug messages linked to member
        for idx, message in enumerate(bug.messages):
            if message.owner_link != person_link:
                continue # If the member is not involved in the message, we can skip it

            event_id = f"{bug.id}-message-{idx}"
            event_time_utc = message.date_created.isoformat()
            relation_type = 'bug_message'

            property_mappings = [
                (message.web_link, 'link'),
                (message.owner_link, 'owner'),
                (message.content, 'content'),
                (message.subject, 'subject'),
            ]
            # Filter out None/empty values and create dictionary
            relation_properties = {}
            relation_properties.update({key: value for value, key in property_mappings if value})

            message_info = {
                'parent_item_id': parent_item_id,
                'event_id': event_id,
                'relation_type': relation_type,
                'employee_id': query.member,
                'event_time_utc': event_time_utc,
                'time_zone': time_zone,
                'relation_properties': relation_properties,
                'event_properties': event_properties
            }
            events.append(message_info)

    return events


@activity.defn
async def transform_data(events: List[dict]) -> List[Event]:
    print(events[0])
    source_kind_id = "launchpad"
    event_type = "bug"
    return [Event(
        id=                     None,  # ID will be assigned by the database
        source_kind_id=         source_kind_id,
        parent_item_id=         e['parent_item_id'],
        event_id=               e['event_id'],

        event_type=             event_type,
        relation_type=          e['relation_type'],

        employee_id=            e['employee_id'],

        event_time_utc=         e['event_time_utc'],
        week=                   None, # Calculated in __post_init__
        timezone=               e.get('time_zone', 'UTC'),
        event_time=             None, # Calculated in __post_init__

        event_properties=       e.get('event_properties', {}),
        relation_properties=    e.get('relation_properties', {}),
        metrics=                e.get('metrics', {})
    ) for e in events]


@activity.defn
async def load_data(events: List[Event]) -> int:
    # TODO: Implement actual database loading logic
    # The best method will probably be to create jobs in a db write queue 
    # to allow for parallel loading to single database
    print(f"Loading {len(events)} bug events into the database...")
    for event in events:
        print(f"Loading event: {event.event_id}")
    return len(events)  # Return the number of events loaded