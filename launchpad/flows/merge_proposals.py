from datetime import datetime
import pytz
import requests
from typing import List, Dict, Any

from models.date_utils import date_in_range, dates_in_range

from launchpad.query import LaunchpadQuery

from launchpadlib.launchpad import Launchpad

import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


merge_proposal_status = [
    'Work in progress',
    'Needs review',
    'Approved',
    'Rejected',
    'Merged',
    'Code failed to merge',
    'Queued',
    'Superseded'
]


async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    logger.info("Extracting Launchpad merge proposal data for member: %s", query.member)
    lp = Launchpad.login_anonymously(consumer_name=query.application_name, service_root=query.service_root, version=query.version)
    if not lp:
        raise ValueError("Failed to connect to Launchpad API")
    
    try:
        person = lp.people[query.member] # type: ignore
    except KeyError:
        return []
    except Exception as e:
        raise ValueError(f"Error fetching member %s: %s", query.member, e)

    merge_proposals = person.getMergeProposals(status=merge_proposal_status)
    if not merge_proposals: return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    time_zone = person.time_zone

    events = []
    logger.info("Found %d merge proposals for member %s", len(merge_proposals), query.member)
    for merge_proposal in merge_proposals:
        logger.info("Processing merge proposal: %s", merge_proposal.self_link)
        parent_item_id = f"mp-{merge_proposal.source_git_path}-{merge_proposal.self_link.split('/')[-1]}"  # mp-<project>/<branch>-<id>

        dates = [
            merge_proposal.date_created, 
            merge_proposal.date_review_requested,
            merge_proposal.date_reviewed, 
            merge_proposal.date_merged
        ]

        comments_response = requests.get(merge_proposal.all_comments_collection_link)
        if comments_response.status_code == 200:
            dates.extend(datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z") for comment in comments_response.json()['entries'])
        if not dates_in_range(dates, from_date, to_date): continue # skip if no dates are in range
        del dates

        event_properties = extract_event_props(merge_proposal)
        metrics = {
            'comments_count': comments_response.json()['total_size'] if comments_response.status_code == 200 else 0,
        }
        relation_properties = {}

        # Create merge_proposal_created event_relation
        if date_in_range(merge_proposal.date_created, from_date, to_date):
            event_id = f"{parent_item_id}-c"
            event_time_utc = merge_proposal.date_created.isoformat()
            employee_id = merge_proposal.registrant_link.split('~')[-1] if merge_proposal.registrant_link else query.member
            relation_type = "merge_proposal_created"
            info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          employee_id,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              metrics
            }
            events.append(info)

        # Create review_requested event_relation
        if date_in_range(merge_proposal.date_review_requested, from_date, to_date):
            event_id = f"{parent_item_id}-rq"
            event_time_utc = merge_proposal.date_review_requested.isoformat()
            relation_type = "merge_proposal_review_requested"
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

        # Create reviewed event_relation
        if date_in_range(merge_proposal.date_reviewed, from_date, to_date):
            event_id = f"{parent_item_id}-r"
            event_time_utc = merge_proposal.date_reviewed.isoformat()
            relation_type = "merge_proposal_reviewed"
            employee_id = merge_proposal.reviewer_link.split('~')[-1] if merge_proposal.reviewer_link else query.member
            relation_properties = extract_merge_proposal_reviewed_relation_props(merge_proposal)
            info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          employee_id,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              metrics
            }
            events.append(info)

        # Create merged event_relation
        if date_in_range(merge_proposal.date_merged, from_date, to_date):
            event_id = f"{parent_item_id}-m"
            event_time_utc = merge_proposal.date_merged.isoformat()
            relation_type = "merge_proposal_merged"
            employee_id = merge_proposal.merge_reporter_link.split('~')[-1] if merge_proposal.merge_reporter_link else query.member
            relation_properties = extract_merge_proposal_merged_relation_props(merge_proposal)
            info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          employee_id,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              metrics
            }
            events.append(info)

        if comments_response.status_code != 200:
            continue # No comments were found, skip to next merge proposal

        for comment in comments_response.json()['entries']:
            date_created = datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z")
            if not date_in_range(date_created, from_date, to_date):
                continue # Skip comments outside the date range

            employee_id = comment['author_link'].split('~')[-1]
            relation_properties = extract_merge_proposal_comment_relation_props(comment)
            event_time_utc = date_created.isoformat()

            # Create comment event_relation
            event_id = f"{parent_item_id}-v{comment['id']}" if comment['vote'] else f"{parent_item_id}-c{comment['id']}"
            relation_type = 'merge_proposal_vote'           if comment['vote'] else 'merge_proposal_comment'
            info = {
                'parent_item_id':       parent_item_id,
                'event_id':             event_id,
                'relation_type':        relation_type,
                'employee_id':          employee_id,
                'event_time_utc':       event_time_utc,
                'time_zone':            time_zone,
                'relation_properties':  relation_properties,
                'event_properties':     event_properties,
                'metrics':              metrics
            }
            events.append(info)

    return events


"""
Helper functions to extract properties from bug, activity, and message objects
"""
def extract_event_props(merge_proposal) -> dict:
    property_mappings = [
        (merge_proposal.address, 'address'),
        (merge_proposal.description, 'description'),
        (merge_proposal.prerequisite_branch_link, 'prerequisite_branch_link'),
        (merge_proposal.prerequisite_git_repository_link, 'prerequisite_git_repository_link'), 
        (merge_proposal.preview_diffs_collection_link, 'preview_diffs_collection_link'),
        (merge_proposal.private, 'private'),
        (merge_proposal.queue_status, 'status'),
        (merge_proposal.web_link, 'link'),
        (merge_proposal.source_branch_link, 'source_branch_link'),
        (merge_proposal.source_git_repository_link, 'source_git_repository_link'),
        (merge_proposal.superseded_by_link, 'superseded_by_link'),
        (merge_proposal.supersedes_link, 'supersedes_link'),
        (merge_proposal.target_branch_link, 'target_branch_link'),
        (merge_proposal.target_git_repository_link, 'target_git_repository_link'),
    ]
    event_properties = {}
    event_properties.update({key: value for value, key in property_mappings if value})
    return event_properties

def extract_merge_proposal_reviewed_relation_props(merge_proposal) -> dict:
    property_mappings = [
        (merge_proposal.reviewed_revid, 'reviewed_revid'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})
    return relation_properties

def extract_merge_proposal_merged_relation_props(merge_proposal) -> dict:
    property_mappings = [
        (merge_proposal.merged_revision_id, 'merged_revision_id'),
        (merge_proposal.merged_revno, 'merged_revno'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})
    return relation_properties

def extract_merge_proposal_comment_relation_props(comment) -> dict:
    property_mappings = [
        (comment['web_link'], 'link'),
        (comment['title'], 'title'),
        (comment['content'], 'content'),
        (comment['date_last_edited'], 'date_last_edited'),
        (comment['date_deleted'], 'date_deleted'),
        (comment['vote'], 'vote'),
        (comment['vote_tag'], 'vote_tag'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})

    return relation_properties