import re
from typing import List, Dict

from external.salesforce.client import SalesforceClient
from external.trino.client import TrinoClient

from models.event import Event
from models.logger import logger
from models.etl.transform_strategy import transform_method

from sources.jira.utils import JiraUtils
from sources.jira.query import JiraQuery


@transform_method("jira")
def transform_data(events: List[Dict]) -> List[Event]:
    """Transform the jira data as per the requirements."""
    # These are going to be chonky dicts
    # Keep the refs while activity executes
    # This allows us to avoid passing them around
    global hrc_email_id_map
    global jira_id_email_map

    hrc_email_id_map = SalesforceClient.get_all_email_employee_ids()
    jira_id_email_map = TrinoClient.get_all_users()

    transformed = []
    for event in events:
        extraction_id = event.get("employee_id")
        if not extraction_id:
            logger.warning(f"Skipping {event['event_id']}: missing employee ID")
            continue

        try:
            is_worklog = event["event_type"] in ["worklog_created", "worklog_updated"]
            employee_id = extract_employee_id(extraction_id)

            e = Event(
                id=None,  # Assigned by the database
                source_kind_id="jira",
                parent_item_id=str(event["parent_item_id"]),
                event_id=event["event_id"],
                event_type=event["event_type"],
                relation_type=event["relation_type"],
                employee_id=employee_id,
                event_time_utc=event["event_time_utc"],
                week=None,  # Calculated in __post_init__
                timezone=event.get("time_zone", "UTC"),
                event_time=event.get("event_time", None),
                event_properties=event.get("event_properties", {}),
                relation_properties=event.get("relation_properties", {}),
                metrics=event.get("metrics", {}),
                version=JiraQuery.version(),
                specific_version=JiraQuery.specific_version(),
            )

            if e.event_type == "assignee_changed":
                transform_assignee_change(e)
            elif e.event_type in ["acceptance_changed", "description_changed"]:
                transform_description_and_acceptance_change(e)
            elif e.event_properties.get("mentions"):
                transform_jira_mentions(e)

            transformed.append(e)
        except Exception as ex:
            logger.error(f"Error transforming {event.get('event_id', 'unknown')}: {ex}")
            continue

    # Clean up global references
    del hrc_email_id_map
    del jira_id_email_map

    return transformed


def transform_assignee_change(event: Event):
    """Transform an assignee change event to map Jira IDs to HRC employee IDs."""
    change = event.event_properties.get("change")
    if not change:
        return

    from_hrc_id = translate_jira_id_to_hrc_id(change.get("from"))
    if from_hrc_id:
        event.event_properties["change"]["from"] = from_hrc_id
    to_hrc_id = translate_jira_id_to_hrc_id(change.get("to"))
    if to_hrc_id:
        event.event_properties["change"]["to"] = to_hrc_id


def transform_description_and_acceptance_change(event: Event):
    change = event.event_properties.get("change")
    if not change:
        return

    # e.g. [~accountid:712020:3db68bf2-18ce-4a92-8954-72b9dcd76c86]
    pattern = r"\[~accountid:([0-9]+:[a-f0-9-]{36})\]"

    def change_mention(field: str):
        if not change.get(field):
            return
        new, mentions = JiraUtils.extract_mentions(change[field], pattern)
        event.event_properties["change"][field] = new
        if mentions:
            event.event_properties[f"{field}_mentions"] = [
                extract_employee_id(m) for m in mentions
            ]

    change_mention("from")
    change_mention("to")


def transform_jira_mentions(event: Event):
    """Transform Jira mentions in the event properties to HRC employee IDs."""
    # If we can't translate, keep the original mention (jira id)
    hrc_mentions = []
    for mention in event.event_properties["mentions"]:
        hrc_id = extract_employee_id(mention)
        hrc_mentions.append(hrc_id if hrc_id else mention)

    event.event_properties["mentions"] = hrc_mentions


# TODO: Look into creating intermediate translation DB
def extract_employee_id(extraction_id: str) -> str:
    """
    Extract the employee ID from the extraction ID.
    If no match is found, return the original extraction ID.
    """
    extraction_id = extraction_id.strip()

    if not "@" in extraction_id:
        return translate_jira_id_to_hrc_id(extraction_id) or extraction_id

    extraction_id = re.sub(r"\+[^@]+@", "@", extraction_id)
    employee_id = hrc_email_id_map.get(extraction_id)

    return employee_id or extraction_id


def translate_jira_id_to_hrc_id(jira_id: str | None) -> str | None:
    """
    Translate a Jira user ID to an HRC employee ID using email as an intermediary.
    """
    if not jira_id:
        return None
    email = jira_id_email_map.get(jira_id)
    return hrc_email_id_map.get(email) if email else None
