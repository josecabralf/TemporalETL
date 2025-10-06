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
    if not query.member:
        logger.warning("No member specified in query")
        return []

    logger.info("Extracting Launchpad merge proposal data for member: %s", query.member)
    lp = LaunchpadConfiguration.get_launchpad_instance()
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")

    lp_user = get_user(query.member, lp)
    if not lp_user:
        return []

    logger.info("Connected to Launchpad member: %s", query.member)
    merge_proposals = lp_user.getMergeProposals(status=merge_proposal_status)
    if not merge_proposals:
        return []

    from_date = datetime.strptime(query.date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

    events = []
    logger.info(
        "Found %d merge proposals for member %s", len(merge_proposals), query.member
    )
    person = Person(query.member, lp_user.time_zone, lp_user.self_link)
    for merge_proposal in merge_proposals:
        logger.info("Processing merge proposal: %s", merge_proposal.self_link)
        events.extend(
            extract_merge_proposal_events(person, merge_proposal, from_date, to_date)
        )

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""


def extract_merge_proposal_events(
    person: Person,
    merge_proposal,
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
            isoparse(comment["date_created"])
            for comment in comments_response.json()["entries"]
        )
    if not dates_in_range(dates, from_date, to_date):
        return events_batch  # skip if no dates are in range

    parent_item_id = f"mp-{merge_proposal.source_git_path}-{merge_proposal.self_link.split('/')[-1]}"  # mp-<project>/<branch>-<id>

    # Create merge_proposal_created event_relation
    if date_in_range(merge_proposal.date_created, from_date, to_date):
        employee_id = (
            merge_proposal.registrant_link.split("~")[-1]
            if merge_proposal.registrant_link
            else person.name
        )

        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-c",
                "event_type": "merge_proposal_created",
                "relation_type": "creator",
                "employee_id": employee_id,
                "event_time_utc": merge_proposal.date_created.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_created(merge_proposal),
            }
        )

    # Create review_requested event_relation
    if date_in_range(merge_proposal.date_review_requested, from_date, to_date):
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-rq",
                "event_type": "merge_proposal_review_requested",
                "relation_type": "requester",
                "employee_id": person.name,
                "event_time_utc": merge_proposal.date_review_requested.isoformat(),
                "time_zone": person.timezone,
                "event_properties": base_event_props(merge_proposal),
            }
        )

    # Create reviewed event_relation
    if date_in_range(merge_proposal.date_reviewed, from_date, to_date):
        employee_id = (
            merge_proposal.reviewer_link.split("~")[-1]
            if merge_proposal.reviewer_link
            else person.name
        )

        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-r",
                "event_type": "merge_proposal_reviewed",
                "relation_type": "reviewer",
                "employee_id": employee_id,
                "event_time_utc": merge_proposal.date_reviewed.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_reviewed(merge_proposal),
            }
        )

    # Create merged event_relation
    if date_in_range(merge_proposal.date_merged, from_date, to_date):
        employee_id = (
            merge_proposal.merge_reporter_link.split("~")[-1]
            if merge_proposal.merge_reporter_link
            else person.name
        )

        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": f"{parent_item_id}-m",
                "event_type": "merge_proposal_merged",
                "relation_type": "merger",
                "employee_id": employee_id,
                "event_time_utc": merge_proposal.date_merged.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_merged(merge_proposal),
            }
        )

    if comments_response.status_code != 200:
        return events_batch  # No comments were found, skip to next merge proposal

    for comment in comments_response.json()["entries"]:
        date_created = isoparse(comment["date_created"])
        if not date_in_range(date_created, from_date, to_date):
            continue  # Skip comments outside the date range

        employee_id = comment["author_link"].split("~")[-1]
        event_id = (
            f"{parent_item_id}-v{comment['id']}"
            if comment["vote"]
            else f"{parent_item_id}-c{comment['id']}"
        )
        event_type = (
            "merge_proposal_vote" if comment["vote"] else "merge_proposal_comment"
        )
        relation_type = "voter" if comment["vote"] else "commenter"

        # Create comment event_relation
        events_batch.append(
            {
                "parent_item_id": parent_item_id,
                "event_id": event_id,
                "event_type": event_type,
                "relation_type": relation_type,
                "employee_id": employee_id,
                "event_time_utc": date_created.isoformat(),
                "time_zone": person.timezone,
                "event_properties": extract_comment(comment, merge_proposal),
            }
        )

    logger.info(
        f"Extracted {len(events_batch)} events for merge proposal {parent_item_id} ({person.name})"
    )
    return events_batch


def base_event_props(mp) -> dict:
    mp_id = f"{mp.source_git_path}-{mp.self_link.split('/')[-1]}"
    return {
        "merge_proposal_id": mp_id,
    }


def extract_created(merge_proposal) -> dict:
    return {
        **base_event_props(merge_proposal),
        "description": merge_proposal.description,
        "prerequisite_branch_link": merge_proposal.prerequisite_branch_link,
        "prerequisite_git_repository_link": merge_proposal.prerequisite_git_repository_link,
        "preview_diffs_collection_link": merge_proposal.preview_diffs_collection_link,
        "private": merge_proposal.private,
        "status": merge_proposal.queue_status,
        "link": merge_proposal.web_link,
        "source_branch_link": merge_proposal.source_branch_link,
        "source_git_repository_link": merge_proposal.source_git_repository_link,
        "superseded_by_link": merge_proposal.superseded_by_link,
        "supersedes_link": merge_proposal.supersedes_link,
        "target_branch_link": merge_proposal.target_branch_link,
        "target_git_repository_link": merge_proposal.target_git_repository_link,
    }


def extract_reviewed(merge_proposal) -> dict:
    return {
        **base_event_props(merge_proposal),
        "reviewed_revid": merge_proposal.reviewed_revid,
    }


def extract_merged(merge_proposal) -> dict:
    return {
        **base_event_props(merge_proposal),
        "merged_revision_id": merge_proposal.merged_revision_id,
        "merged_revno": merge_proposal.merged_revno,
    }


def extract_comment(comment: dict, merge_proposal) -> dict:
    return {
        **base_event_props(merge_proposal),
        "link": comment.get("web_link"),
        "title": comment.get("title"),
        "content": comment.get("content"),
        "date_last_edited": comment.get("date_last_edited"),
        "date_deleted": comment.get("date_deleted"),
        "vote": comment.get("vote"),
        "vote_tag": comment.get("vote_tag"),
    }
