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
    lp = Launchpad.login_anonymously(
        consumer_name=query.application_name,
        service_root=query.service_root,
        version=query.version,
    )
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    try:
        person = lp.people[query.member]  # type: ignore
    except KeyError:
        return []  # Member does not exist, return empty list
    except Exception as e:
        raise ValueError(
            f"Error fetching member %s: %s", query.member, e
        )  # Handle other exceptions

    merge_proposals = person.getMergeProposals(status=merge_proposal_status)
    if not merge_proposals:
        return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(
        tzinfo=pytz.UTC
    )
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(
        tzinfo=pytz.UTC
    )
    time_zone = person.time_zone

    events = []
    logger.info(
        "Found %d merge proposals for member %s", len(merge_proposals), query.member
    )
    for merge_proposal in merge_proposals:
        logger.info("Processing merge proposal: %s", merge_proposal.self_link)
        events.extend(
            extract_merge_proposal_events(
                query, merge_proposal, time_zone, from_date, to_date
            )
        )

    return events


@extract_method(name="launchpad-merge_proposals-streaming")
async def extract_data_streaming(
    query: LaunchpadQuery, chunk_size: int
) -> AsyncIterator[List[Dict[str, Any]]]:
    logger.info(
        "Streaming extraction of Launchpad merge proposals data for member: %s with chunk size: %d",
        query.member,
        chunk_size,
    )
    lp = Launchpad.login_anonymously(
        consumer_name=query.application_name,
        service_root=query.service_root,
        version=query.version,
    )
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    try:
        person = lp.people[query.member]  # type: ignore
    except KeyError:
        return  # Member does not exist, return empty iterator
    except Exception as e:
        raise ValueError(
            f"Error fetching member {query.member}: {e}"
        )  # Handle other exceptions

    # Fetch merge proposals based on date range and status
    merge_proposals = person.getMergeProposals(status=merge_proposal_status)
    if not merge_proposals:
        return

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(
        tzinfo=pytz.UTC
    )
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(
        tzinfo=pytz.UTC
    )
    time_zone = person.time_zone

    # Process merge proposals in truly streaming fashion - yield each chunk as it's processed
    chunk_count = 0
    logger.info(
        "Found %d merge proposals for member %s, processing in streaming chunks of %d",
        len(merge_proposals),
        query.member,
        chunk_size,
    )
    for i in range(0, len(merge_proposals), chunk_size):
        batch = merge_proposals[i : i + chunk_size]
        events_batch = []

        chunk_count += 1
        logger.info(
            f"Processing chunk {chunk_count}: merge proposals {i + 1} to {min(i + chunk_size, len(merge_proposals))} of {len(merge_proposals)}"
        )

        for merge_proposal in batch:
            events_batch.extend(
                extract_merge_proposal_events(
                    query, merge_proposal, time_zone, from_date, to_date
                )
            )

        logger.info(f"Chunk {chunk_count} produced {len(events_batch)} events")
        # Yield the chunk immediately instead of accumulating
        yield events_batch


setattr(extract_data, "is_streaming", False)
setattr(extract_data_streaming, "is_streaming", True)

"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_merge_proposal_events(
    query: LaunchpadQuery,
    merge_proposal,
    time_zone: str,
    from_date: datetime,
    to_date: datetime,
) -> List[Dict[str, Any]]:
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
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-c",
                "relation_type": "merge_proposal_created",
                "employee_id": merge_proposal.registrant_link.split("~")[-1]
                if merge_proposal.registrant_link
                else query.member,
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
                "employee_id": query.member,
                "event_time_utc": merge_proposal.date_review_requested.isoformat(),
                "time_zone": time_zone,
                "relation_properties": {},
                "event_properties": event_properties,
                "metrics": metrics,
            }
        )

    # Create reviewed event_relation
    if date_in_range(merge_proposal.date_reviewed, from_date, to_date):
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-r",
                "relation_type": "merge_proposal_reviewed",
                "employee_id": merge_proposal.reviewer_link.split("~")[-1]
                if merge_proposal.reviewer_link
                else query.member,
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
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-m",
                "relation_type": "merge_proposal_merged",
                "employee_id": merge_proposal.merge_reporter_link.split("~")[-1]
                if merge_proposal.merge_reporter_link
                else query.member,
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
                "employee_id": comment["author_link"].split("~")[-1],
                "event_time_utc": date_created.isoformat(),
                "time_zone": time_zone,
                "relation_properties": extract_merge_proposal_comment_relation_props(
                    comment
                ),
                "event_properties": event_properties,
                "metrics": metrics,
            }
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
