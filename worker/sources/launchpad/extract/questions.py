from datetime import datetime
from dateutil.parser import isoparse
import pytz
import requests
from typing import Any, Dict, List


from models.date_utils import date_in_range, dates_in_range
from models.etl.extract_strategy import extract_method
from models.logger import logger

from sources.launchpad.query import LaunchpadQuery
from sources.launchpad.config import LaunchpadConfiguration
from sources.launchpad.person import Person, get_user


@extract_method(name="launchpad-questions")
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    if not query.member:
        logger.warning("No member specified in query")
        return []

    query.member = query.member.lower()
    logger.info("Extracting Launchpad question data for member: %s", query.member)
    lp = LaunchpadConfiguration.get_launchpad_instance()
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    lp_user = get_user(query.member, lp)
    if not lp_user:
        return []

    logger.info("Connected to Launchpad member: %s", query.member)
    questions = lp_user.searchQuestions(participation="Owner")
    if not questions:
        return []

    from_date = datetime.strptime(query.date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

    events = []
    logger.info("Found %d questions for member %s", len(questions), query.member)
    person = Person(query.member, lp_user.time_zone, lp_user.self_link)
    for question in questions:
        logger.info("Processing question: %s", question.self_link)
        events.extend(extract_question_events(person, question, from_date, to_date))

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_question_events(
    person: Person,
    question,
    from_date: datetime,
    to_date: datetime,
) -> List[Dict[str, Any]]:
    batch_events = []
    dates = [
        question.date_created,
        question.date_last_query,
        question.date_last_response,
        question.date_solved,
    ]

    answers_response = requests.get(question.messages_collection_link)
    if answers_response.status_code == 200:
        dates.extend(
            isoparse(comment["date_created"])
            for comment in answers_response.json()["entries"]
        )
    if not dates_in_range(dates, from_date, to_date):
        return batch_events  # Skip if no dates are in range

    parent_item_id = f"q-{question.id}"

    if date_in_range(question.date_created, from_date, to_date):
        batch_events.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-c",
                "event_type": "question_created",
                "relation_type": "owner",
                "employee_id": person.name,
                "event_time_utc": question.date_created.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_created(question),
            }
        )

    if answers_response.status_code != 200:
        return batch_events  # No answers were found, skip to next question

    logger.info(
        "Processing %d answers for question %s",
        answers_response.json()["total_size"],
        question.id,
    )
    for answer in answers_response.json()["entries"]:
        answer_date = isoparse(answer["date_created"])
        if not date_in_range(answer_date, from_date, to_date):
            continue

        employee_id = answer["owner_link"].split("~")[-1]
        is_solved = answer.get("new_status") == "Solved"
        event_id = f"{parent_item_id}-{'s' if is_solved else 'a'}{answer['index']}"
        event_type = "question_solved" if is_solved else "question_answered"
        batch_events.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": event_id,
                "event_type": event_type,
                "relation_type": "author",
                "employee_id": employee_id,
                "event_time_utc": answer_date.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_answer(answer, question.id),
            }
        )

    logger.info(
        f"Extracted {len(batch_events)} events for question {parent_item_id} ({person.name})"
    )
    return batch_events


def base_event_props(question_id: str) -> dict:
    return {
        "question_id": question_id,
    }


def extract_created(question) -> dict:
    link_split = question.assignee_link.split("~") if question.assignee_link else []
    assignee = link_split[-1] if link_split else None
    date_due = question.date_due.isoformat() if question.date_due else None

    return {
        **base_event_props(question.id),
        "title": question.title,
        "date_due": date_due,
        "description": question.description,
        "assignee": assignee,
        "language_link": question.language_link,
        "target_link": question.target_link,
        "web_link": question.web_link,
    }


def extract_answer(answer: dict, question_id: str) -> dict:
    return {
        **base_event_props(question_id),
        "link": answer.get("web_link"),
        "content": answer.get("content"),
        "subject": answer.get("subject"),
        "bug_attachments_collection_link": answer.get(
            "bug_attachments_collection_link"
        ),
        "question_link": answer.get("question_link"),
        "action": answer.get("action"),
        "new_status": answer.get("new_status"),
    }
