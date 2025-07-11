from datetime import datetime
from typing import Any, Dict, List

import pytz
import requests

from launchpadlib.launchpad import Launchpad

from models.date_utils import date_in_range, dates_in_range
from models.etl.extract_cmd import extract_method
from models.logger import logger

from sources.launchpad.query import LaunchpadQuery
from sources.launchpad.config import LaunchpadConfiguration


merge_proposal_status = [
    "Work in progress",
    "Needs review",
    "Approved",
    "Rejected",
    "Merged",
    "Code failed to merge",
    "Queued",
    "Superseded",
]


@extract_method(name="launchpad-merge_proposals")
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    logger.info("Extracting Launchpad merge proposal data for member: %s", query.member)
    lp = LaunchpadConfiguration.get_launchpad_instance()
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    try:
        person = lp.people[query.member]  # type: ignore
    except KeyError:
        return []  # Member does not exist, return empty list
    except Exception as e:
        raise ValueError(
            "Error fetching member %s: %s", query.member, e
        )  # Handle other exceptions

    if not person:
        return []

    logger.info("Connected to Launchpad member: %s", person.name)
    merge_proposals = person.getMergeProposals(status=merge_proposal_status)
    if not merge_proposals:
        return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(
        tzinfo=pytz.UTC
    )
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(
        tzinfo=pytz.UTC
    )

    events = []
    logger.info(
        "Found %d merge proposals for member %s", len(merge_proposals), query.member
    )
    for merge_proposal in merge_proposals:
        logger.info("Processing merge proposal: %s", merge_proposal.self_link)
        events.extend(
            extract_merge_proposal_events(
                person, merge_proposal, from_date, to_date, lp
            )
        )

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_merge_proposal_events(
    person,
    merge_proposal,
    from_date: datetime,
    to_date: datetime,
    launchpad: Launchpad,
) -> List[Dict[str, Any]]:
    person_id = person.id
    time_zone = person.time_zone

    events_batch = []
    dates = [
        merge_proposal.date_created,
        merge_proposal.date_review_requested,
        merge_proposal.date_reviewed,
        merge_proposal.date_merged,
    ]

    comments_response = requests.get(merge_proposal.all_comments_collection_link)
    if comments_response.status_code == 200:
        dates.extend(
            datetime.strptime(comment["date_created"], "%Y-%m-%dT%H:%M:%S.%f%z")
            for comment in comments_response.json()["entries"]
        )
    if not dates_in_range(dates, from_date, to_date):
        return events_batch  # skip if no dates are in range

    parent_item_id = f"mp-{merge_proposal.source_git_path}-{merge_proposal.self_link.split('/')[-1]}"  # mp-<project>/<branch>-<id>
    event_properties = extract_merge_proposal_event_props(merge_proposal)
    metrics = extract_merge_proposal_metrics(merge_proposal)

    # Create merge_proposal_created event_relation
    if date_in_range(merge_proposal.date_created, from_date, to_date):
        employee_id = person_id
        event_owner = (
            merge_proposal.registrant_link.split("~")[-1]
            if merge_proposal.registrant_link
            else None
        )
        if event_owner:
            try:
                employee_id = launchpad.people[event_owner].id  # type: ignore
            except KeyError:
                logger.warning(
                    f"Registrant {event_owner} not found in Launchpad, using person ID {person_id}"
                )
            except Exception as e:
                logger.error(
                    f"Error fetching registrant {event_owner}: {e}, using person ID {person_id}"
                )

        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-c",
                "relation_type": "merge_proposal_created",
                "employee_id": employee_id,
                "event_time_utc": merge_proposal.date_created.isoformat(),
                "time_zone": time_zone,
                "relation_properties": {},
                "event_properties": event_properties,
                "metrics": metrics,
            }
        )

    # Create review_requested event_relation
    if date_in_range(merge_proposal.date_review_requested, from_date, to_date):
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-rq",
                "relation_type": "merge_proposal_review_requested",
                "employee_id": person_id,
                "event_time_utc": merge_proposal.date_review_requested.isoformat(),
                "time_zone": time_zone,
                "relation_properties": {},
                "event_properties": event_properties,
                "metrics": metrics,
            }
        )

    # Create reviewed event_relation
    if date_in_range(merge_proposal.date_reviewed, from_date, to_date):
        employee_id = person_id
        event_owner = (
            merge_proposal.reviewer_link.split("~")[-1]
            if merge_proposal.reviewer_link
            else None
        )
        if event_owner:
            try:
                employee_id = launchpad.people[event_owner].id  # type: ignore
            except KeyError:
                logger.warning(
                    f"Registrant {event_owner} not found in Launchpad, using person ID {person_id}"
                )
            except Exception as e:
                logger.error(
                    f"Error fetching registrant {event_owner}: {e}, using person ID {person_id}"
                )

        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-r",
                "relation_type": "merge_proposal_reviewed",
                "employee_id": employee_id,
                "event_time_utc": merge_proposal.date_reviewed.isoformat(),
                "time_zone": time_zone,
                "relation_properties": extract_merge_proposal_reviewed_relation_props(
                    merge_proposal
                ),
                "event_properties": event_properties,
                "metrics": metrics,
            }
        )

    # Create merged event_relation
    if date_in_range(merge_proposal.date_merged, from_date, to_date):
        employee_id = person_id
        event_owner = (
            merge_proposal.merge_reporter_link.split("~")[-1]
            if merge_proposal.merge_reporter_link
            else None
        )
        if event_owner:
            try:
                employee_id = launchpad.people[event_owner].id  # type: ignore
            except KeyError:
                logger.warning(
                    f"Registrant {event_owner} not found in Launchpad, using person ID {person_id}"
                )
            except Exception as e:
                logger.error(
                    f"Error fetching registrant {event_owner}: {e}, using person ID {person_id}"
                )

        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-m",
                "relation_type": "merge_proposal_merged",
                "employee_id": employee_id,
                "event_time_utc": merge_proposal.date_merged.isoformat(),
                "time_zone": time_zone,
                "relation_properties": extract_merge_proposal_merged_relation_props(
                    merge_proposal
                ),
                "event_properties": event_properties,
                "metrics": metrics,
            }
        )

    if comments_response.status_code != 200:
        return events_batch  # No comments were found, skip to next merge proposal

    for comment in comments_response.json()["entries"]:
        date_created = datetime.strptime(
            comment["date_created"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        if not date_in_range(date_created, from_date, to_date):
            continue  # Skip comments outside the date range

        employee_id = person_id
        event_owner = comment["author_link"].split("~")[-1]
        try:
            employee_id = launchpad.people[event_owner].id  # type: ignore
        except KeyError:
            logger.warning(
                f"Registrant {event_owner} not found in Launchpad, using person ID {person_id}"
            )
        except Exception as e:
            logger.error(
                f"Error fetching registrant {event_owner}: {e}, using person ID {person_id}"
            )

        # Create comment event_relation
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-v{comment['id']}"
                if comment["vote"]
                else f"{parent_item_id}-c{comment['id']}",
                "relation_type": "merge_proposal_vote"
                if comment["vote"]
                else "merge_proposal_comment",
                "employee_id": employee_id,
                "event_time_utc": date_created.isoformat(),
                "time_zone": time_zone,
                "relation_properties": extract_merge_proposal_comment_relation_props(
                    comment
                ),
                "event_properties": event_properties,
                "metrics": metrics,
            }
        )

    logger.info(
        f"Extracted {len(events_batch)} events for merge proposal {parent_item_id} ({person_id})"
    )
    return events_batch


def extract_merge_proposal_event_props(merge_proposal) -> dict:
    return {
        "address": merge_proposal.address
        if hasattr(merge_proposal, "address")
        else None,
        "description": merge_proposal.description
        if hasattr(merge_proposal, "description")
        else None,
        "prerequisite_branch_link": merge_proposal.prerequisite_branch_link
        if hasattr(merge_proposal, "prerequisite_branch_link")
        else None,
        "prerequisite_git_repository_link": merge_proposal.prerequisite_git_repository_link
        if hasattr(merge_proposal, "prerequisite_git_repository_link")
        else None,
        "preview_diffs_collection_link": merge_proposal.preview_diffs_collection_link
        if hasattr(merge_proposal, "preview_diffs_collection_link")
        else None,
        "private": merge_proposal.private
        if hasattr(merge_proposal, "private")
        else None,
        "status": merge_proposal.queue_status
        if hasattr(merge_proposal, "queue_status")
        else None,
        "link": merge_proposal.web_link
        if hasattr(merge_proposal, "web_link")
        else None,
        "source_branch_link": merge_proposal.source_branch_link
        if hasattr(merge_proposal, "source_branch_link")
        else None,
        "source_git_repository_link": merge_proposal.source_git_repository_link
        if hasattr(merge_proposal, "source_git_repository_link")
        else None,
        "superseded_by_link": merge_proposal.superseded_by_link
        if hasattr(merge_proposal, "superseded_by_link")
        else None,
        "supersedes_link": merge_proposal.supersedes_link
        if hasattr(merge_proposal, "supersedes_link")
        else None,
        "target_branch_link": merge_proposal.target_branch_link
        if hasattr(merge_proposal, "target_branch_link")
        else None,
        "target_git_repository_link": merge_proposal.target_git_repository_link
        if hasattr(merge_proposal, "target_git_repository_link")
        else None,
    }


def extract_merge_proposal_metrics(merge_proposal) -> dict:
    return {
        "queue_position": merge_proposal.queue_position
        if hasattr(merge_proposal, "queue_position") and merge_proposal.queue_position
        else None,
    }


def extract_merge_proposal_reviewed_relation_props(merge_proposal) -> dict:
    return {
        "reviewed_revid": merge_proposal.reviewed_revid
        if hasattr(merge_proposal, "reviewed_revid")
        else None,
    }


def extract_merge_proposal_merged_relation_props(merge_proposal) -> dict:
    return {
        "merged_revision_id": merge_proposal.merged_revision_id
        if hasattr(merge_proposal, "merged_revision_id")
        else None,
        "merged_revno": merge_proposal.merged_revno
        if hasattr(merge_proposal, "merged_revno")
        else None,
    }


def extract_merge_proposal_comment_relation_props(comment: dict) -> dict:
    return {
        "link": comment.get("web_link"),
        "title": comment.get("title"),
        "content": comment.get("content"),
        "date_last_edited": comment.get("date_last_edited"),
        "date_deleted": comment.get("date_deleted"),
        "vote": comment.get("vote"),
        "vote_tag": comment.get("vote_tag"),
    }
