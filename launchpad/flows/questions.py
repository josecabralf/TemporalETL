from datetime import datetime
import pytz
import requests
from typing import List, Dict, Any

from models.date_utils import date_in_range, dates_in_range
from models.extract_cmd import extract_method

from launchpad.query import LaunchpadQuery

from launchpadlib.launchpad import Launchpad

import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@extract_method(name="launchpad-questions")
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    logger.info("Extracting Launchpad question data for member: %s", query.member)
    lp = Launchpad.login_anonymously(consumer_name=query.application_name, service_root=query.service_root, version=query.version)
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")
    
    try:
        person = lp.people[query.member] # type: ignore
    except KeyError:
        return []
    except Exception as e:
        raise ValueError(f"Error fetching member %s: %s", query.member, e)
    
    questions = person.searchQuestions(participation='Owner')
    if not questions: return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    time_zone = person.time_zone

    events = []
    logger.info("Found %d questions for member %s", len(questions), query.member)
    for question in questions:
        parent_item_id = f"q-{question.id}"

        dates = [
            question.date_created, 
            question.date_last_query, 
            question.date_last_response, 
            question.date_solved
        ]

        answers_response = requests.get(question.messages_collection_link)
        if answers_response.status_code == 200:
            dates.extend(datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z") for comment in answers_response.json()['entries'])
        if not dates_in_range(dates, from_date, to_date): continue # Skip if no dates are in range

        event_properties = extract_event_props(question)
        metrics = {
            'answers_count': answers_response.json()['total_size'] if answers_response.status_code == 200 else 0,
        }
        relation_properties = {}

        if date_in_range(question.date_created, from_date, to_date):
            event_id = f"{parent_item_id}-c"
            event_time_utc = question.date_created.isoformat()
            relation_type = "question_created"
            info = {
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
            events.append(info)

        if answers_response.status_code != 200:
            continue # No answers were found, skip to next question
        
        logger.info("Processing %d answers for question %s", answers_response.json()['total_size'], question.id)
        for answer in answers_response.json()['entries']:
            answer_date = datetime.strptime(answer['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z")
            if not date_in_range(answer_date, from_date, to_date): continue

            is_solved = answer.get('new_status') == 'Solved'
            event_id = f"{parent_item_id}-{'s' if is_solved else 'a'}{answer['index']}"
            relation_type = "question_solved" if is_solved else "question_answered"

            event_time_utc = answer_date.isoformat()
            employee_id = answer['owner_link'].split('~')[-1]
            relation_properties = extract_answer_relation_props(answer)
            info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          employee_id,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              {}
            }
            events.append(info)

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""
def extract_event_props(question) -> dict:
    property_mappings = [
        (question.title, 'title'),
        (question.description, 'description'),
        (question.language_link, 'language_link'),
        (question.status, 'status'),
        (question.web_link, 'link'),
    ]
    event_properties = {}
    event_properties.update({key: value for value, key in property_mappings if value})
    return event_properties


def extract_answer_relation_props(answer) -> dict:
    property_mappings = [
        (answer['web_link'], 'link'),
        (answer['content'], 'content'),
        (answer['subject'], 'subject'),
        (answer['bug_attachments_collection_link'], 'attachments_link'),
        (answer['question_link'], 'question_link'),
        (answer['action'], 'action'),
        (answer['new_status'], 'new_status'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})
    return relation_properties
