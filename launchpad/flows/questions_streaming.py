from datetime import datetime
import pytz
import requests
from typing import List, Dict, Any, Iterator
import logging

from models.date_utils import date_in_range, dates_in_range
from models.extract_cmd import extract_method
from launchpad.query import LaunchpadQuery
from launchpadlib.launchpad import Launchpad


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@extract_method(name="launchpad-questions-streaming")
async def extract_data_streaming(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    """
    Streaming version of question data extraction that processes data in batches.
    This reduces memory usage for large datasets.
    """
    logger.info("Starting streaming extraction of Launchpad question data for member: %s", query.member)
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
    
    questions = person.searchQuestions(participation='Owner')
    if not questions: 
        logger.info("No questions found for member %s", query.member)
        return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    time_zone = person.time_zone

    logger.info("Found %d questions for member %s", len(questions), query.member)
    
    # Process questions in streaming batches
    all_events = []
    batch_count = 0
    
    for event_batch in process_question_batch_streaming(
        questions, from_date, to_date, query.member, time_zone, batch_size=20
    ):
        all_events.extend(event_batch)
        batch_count += 1
        
        # Log progress for large datasets
        if batch_count % 5 == 0:
            logger.info(f"Processed {batch_count} question batches, {len(all_events)} events so far")
    
    logger.info(f"Streaming question extraction complete: {len(all_events)} total events from {batch_count} batches")
    return all_events

def process_question_batch_streaming(questions: List, from_date: datetime, to_date: datetime, 
                                   member: str, time_zone: str, batch_size: int = 20) -> Iterator[List[Dict[str, Any]]]:
    """
    Process questions in batches to reduce memory usage and enable streaming.
    
    Args:
        questions: List of questions from Launchpad
        from_date: Start date for filtering
        to_date: End date for filtering
        member: Member identifier
        time_zone: Member's timezone
        batch_size: Number of questions to process per batch
        
    Yields:
        Lists of event dictionaries for each batch
    """
    for i in range(0, len(questions), batch_size):
        batch = questions[i:i + batch_size]
        events_batch = []
        
        logger.info(f"Processing question batch {i//batch_size + 1}: questions {i+1} to {min(i+batch_size, len(questions))} of {len(questions)}")
        
        for question in batch:
            logger.debug(f"Processing question: {question.self_link}")
            
            # Collect all relevant dates for this question
            dates = [
                question.date_created, 
                question.date_last_query, 
                question.date_last_response, 
                question.date_solved
            ]

            # Fetch answers for this question
            answers_response = None
            try:
                answers_response = requests.get(question.messages_collection_link, timeout=30)
                if answers_response.status_code == 200:
                    answers_data = answers_response.json()
                    dates.extend(
                        datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z") 
                        for comment in answers_data['entries']
                    )
                else:
                    logger.warning(f"Failed to fetch answers for question {question.id}: HTTP {answers_response.status_code}")
            except Exception as e:
                logger.warning(f"Error fetching answers for question {question.id}: {e}")
                
            # Skip if no dates are in range
            if not dates_in_range(dates, from_date, to_date): 
                continue

            parent_item_id = f"q-{question.id}"
            event_properties = extract_event_props(question)
            
            # Calculate metrics based on answers response
            answers_count = 0
            if answers_response and answers_response.status_code == 200:
                try:
                    answers_data = answers_response.json()
                    answers_count = answers_data['total_size']
                except Exception as e:
                    logger.warning(f"Error parsing answers JSON for question {question.id}: {e}")
                    
            metrics = {
                'answers_count': answers_count,
            }

            # Process question creation event
            if date_in_range(question.date_created, from_date, to_date):
                events_batch.append({
                    'parent_item_id':       parent_item_id,
                    'event_id':             f"{parent_item_id}-c",
                    'relation_type':        "question_created",
                    'employee_id':          member,
                    'event_time_utc':       question.date_created.isoformat(),
                    'time_zone':            time_zone,
                    'relation_properties':  {},
                    'event_properties':     event_properties,
                    'metrics':              metrics
                })

            # Process answers if available
            if answers_response and answers_response.status_code == 200:
                answers_data = answers_response.json()
                logger.debug(f"Processing {answers_data['total_size']} answers for question {question.id}")
                
                for answer in answers_data['entries']:
                    try:
                        answer_date = datetime.strptime(answer['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z")
                        if not date_in_range(answer_date, from_date, to_date): 
                            continue

                        is_solved = answer.get('new_status') == 'Solved'
                        answer_employee_id = answer['owner_link'].split('~')[-1] if answer.get('owner_link') else member
                        
                        events_batch.append({
                            'parent_item_id':       parent_item_id,
                            'event_id':             f"{parent_item_id}-{'s' if is_solved else 'a'}{answer['index']}",
                            'relation_type':        "question_solved" if is_solved else "question_answered",
                            'employee_id':          answer_employee_id,
                            'event_time_utc':       answer_date.isoformat(),
                            'time_zone':            time_zone,
                            'relation_properties':  extract_answer_relation_props(answer),
                            'event_properties':     event_properties,
                            'metrics':              {}
                        })
                    except Exception as e:
                        logger.warning(f"Error processing answer {answer.get('index', 'unknown')} for question {question.id}: {e}")
                        continue
        
        logger.info(f"Question batch {i//batch_size + 1} produced {len(events_batch)} events")
        yield events_batch


"""
Helper functions to extract properties from bug, activity, and message objects
"""
def extract_event_props(question) -> dict:
    """Extract question event properties."""
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
    """Extract answer relation properties."""
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