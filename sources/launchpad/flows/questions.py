from datetime import datetime
import pytz
import requests
from typing import Any, AsyncIterator, Dict, List 

from models.date_utils import date_in_range, dates_in_range
from models.etl.extract_cmd import extract_method

from sources.launchpad.query import LaunchpadQuery

from launchpadlib.launchpad import Launchpad

import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@extract_method(name="launchpad-questions")
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    logger.info("Extracting Launchpad question data for member: %s", query.member)
    lp = Launchpad.login_anonymously(consumer_name=query.application_name, service_root=query.service_root, version=query.version)
    if not lp: raise ValueError("Failed to connect to Launchpad API")
    
    try:                    person = lp.people[query.member] # type: ignore
    except KeyError:        return [] # Member does not exist, return empty list
    except Exception as e:  raise ValueError(f"Error fetching member %s: %s", query.member, e) # Handle other exceptions
    
    questions = person.searchQuestions(participation='Owner')
    if not questions:  return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    time_zone = person.time_zone

    events = []
    logger.info("Found %d questions for member %s", len(questions), query.member)
    for question in questions:
        logger.info("Processing question: %s", question.self_link)
        events.extend(extract_question_events(query, question, time_zone, from_date, to_date))

    return events


@extract_method(name="launchpad-questions-streaming")
async def extract_data_streaming(query: LaunchpadQuery, chunk_size: int) -> AsyncIterator[List[Dict[str, Any]]]:
    logger.info("Streaming extraction of Launchpad question data for member: %s with chunk size: %d", query.member, chunk_size)
    lp = Launchpad.login_anonymously(consumer_name=query.application_name, service_root=query.service_root, version=query.version)
    if not lp: raise ValueError("Failed to connect to Launchpad API")
    
    try:                    person = lp.people[query.member] # type: ignore
    except KeyError:        return # Member does not exist, return empty iterator
    except Exception as e:  raise ValueError(f"Error fetching member {query.member}: {e}") # Handle other exceptions

    questions = person.searchQuestions(participation='Owner')
    if not questions: return

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    time_zone = person.time_zone
    person_link = person.self_link 

    # Process questions in truly streaming fashion - yield each chunk as it's processed
    chunk_count = 0
    logger.info("Found %d questions for member %s, processing in streaming chunks of %d", len(questions), query.member, chunk_size)
    for i in range(0, len(questions), chunk_size):
        batch = questions[i:i + chunk_size]
        events_batch = []
        
        chunk_count += 1
        logger.info(f"Processing chunk {chunk_count}: questions {i+1} to {min(i+chunk_size, len(questions))} of {len(questions)}")
        
        for question in batch:
            events_batch.extend(extract_question_events(query, question, time_zone, from_date, to_date))
        
        logger.info(f"Chunk {chunk_count} produced {len(events_batch)} events")
        # Yield the chunk immediately instead of accumulating
        yield events_batch

setattr(extract_data, "is_streaming", False)
setattr(extract_data_streaming, "is_streaming", True)

"""
Helper functions to extract properties from bug, activity, and message objects
"""
def extract_question_events(query: LaunchpadQuery, question, time_zone: str, from_date: datetime, to_date: datetime) -> List[Dict[str, Any]]:
    batch_events = []
    dates = [
        question.date_created, 
        question.date_last_query, 
        question.date_last_response, 
        question.date_solved
    ]

    answers_response = requests.get(question.messages_collection_link)
    if answers_response.status_code == 200:
        dates.extend(datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z") for comment in answers_response.json()['entries'])
    if not dates_in_range(dates, from_date, to_date): return batch_events # Skip if no dates are in range

    parent_item_id = f"q-{question.id}"
    event_properties = extract_event_props(question)
    metrics = extract_question_metrics(question)

    if date_in_range(question.date_created, from_date, to_date):
        batch_events.append({
            'parent_item_id':       parent_item_id,
            'event_id':             f"{parent_item_id}-c",
            'relation_type':        "question_created",
            'employee_id':          query.member,
            'event_time_utc':       question.date_created.isoformat(),
            'time_zone':            time_zone,
            'relation_properties':  {},
            'event_properties':     event_properties,
            'metrics':              metrics
        })

    if answers_response.status_code != 200: return batch_events # No answers were found, skip to next question
    
    logger.info("Processing %d answers for question %s", answers_response.json()['total_size'], question.id)
    for answer in answers_response.json()['entries']:
        answer_date = datetime.strptime(answer['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z")
        if not date_in_range(answer_date, from_date, to_date): 
            continue

        is_solved = answer.get('new_status') == 'Solved'
        batch_events.append({
            'parent_item_id':       parent_item_id,
            'event_id':             f"{parent_item_id}-{'s' if is_solved else 'a'}{answer['index']}",
            'relation_type':        "question_solved" if is_solved else "question_answered",
            'employee_id':          answer['owner_link'].split('~')[-1],
            'event_time_utc':       answer_date.isoformat(),
            'time_zone':            time_zone,
            'relation_properties':  extract_answer_relation_props(answer),
            'event_properties':     event_properties,
            'metrics':              {}
        })
    
    return batch_events


def extract_event_props(question) -> dict:
    return {
        'title': question.title if hasattr(question, 'title') else None,
        'description': question.description if hasattr(question, 'description') else None,
        'language_link': question.language_link if hasattr(question, 'language_link') else None,
        'status': question.status if hasattr(question, 'status') else None,
        'web_link': question.web_link if hasattr(question, 'web_link') else None,
    }


def extract_question_metrics(question) -> dict:
    return {
        'answer_count': question.answer_count if hasattr(question, 'answer_count') else 0,
    }


def extract_answer_relation_props(answer: dict) -> dict:
    return {
        'link': answer.get('web_link'),
        'content': answer.get('content'),
        'subject': answer.get('subject'),
        'bug_attachments_collection_link': answer.get('bug_attachments_collection_link'),
        'question_link': answer.get('question_link'),
        'action': answer.get('action'),
        'new_status': answer.get('new_status'),
    }