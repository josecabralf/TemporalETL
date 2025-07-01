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

@extract_method(name="launchpad-merge_proposals-streaming")
async def extract_data_streaming(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    """
    Streaming version of merge proposal data extraction that processes data in batches.
    This reduces memory usage for large datasets.
    """
    logger.info("Starting streaming extraction of Launchpad merge proposal data for member: %s", query.member)
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
    
    merge_proposals = person.getMergeProposals(status=merge_proposal_status)
    if not merge_proposals: 
        logger.info("No merge proposals found for member %s", query.member)
        return []

    from_date = datetime.strptime(query.data_date_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    to_date = datetime.strptime(query.data_date_end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    time_zone = person.time_zone

    logger.info("Found %d merge proposals for member %s", len(merge_proposals), query.member)
    
    # Process merge proposals in streaming batches
    all_events = []
    batch_count = 0
    
    for event_batch in process_merge_proposal_batch_streaming(
        merge_proposals, from_date, to_date, query.member, time_zone, batch_size=15
    ):
        all_events.extend(event_batch)
        batch_count += 1
        
        # Log progress for large datasets
        if batch_count % 3 == 0:
            logger.info(f"Processed {batch_count} merge proposal batches, {len(all_events)} events so far")
    
    logger.info(f"Streaming merge proposal extraction complete: {len(all_events)} total events from {batch_count} batches")
    return all_events


def process_merge_proposal_batch_streaming(merge_proposals: List, from_date: datetime, to_date: datetime, 
                                         member: str, time_zone: str, batch_size: int = 15) -> Iterator[List[Dict[str, Any]]]:
    """
    Process merge proposals in batches to reduce memory usage and enable streaming.
    
    Args:
        merge_proposals: List of merge proposals from Launchpad
        from_date: Start date for filtering
        to_date: End date for filtering
        member: Member identifier
        time_zone: Member's timezone
        batch_size: Number of merge proposals to process per batch
        
    Yields:
        Lists of event dictionaries for each batch
    """
    for i in range(0, len(merge_proposals), batch_size):
        batch = merge_proposals[i:i + batch_size]
        events_batch = []
        
        logger.info(f"Processing merge proposal batch {i//batch_size + 1}: proposals {i+1} to {min(i+batch_size, len(merge_proposals))} of {len(merge_proposals)}")
        
        for merge_proposal in batch:
            logger.debug(f"Processing merge proposal: {merge_proposal.self_link}")
            
            # Collect all relevant dates for this merge proposal
            dates = [
                merge_proposal.date_created, 
                merge_proposal.date_review_requested,
                merge_proposal.date_reviewed, 
                merge_proposal.date_merged
            ]
            
            # Fetch comments for this merge proposal
            comments_response = None
            comments_data = None
            try:
                comments_response = requests.get(merge_proposal.all_comments_collection_link, timeout=30)
                if comments_response.status_code == 200:
                    comments_data = comments_response.json()
                    dates.extend(
                        datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z") 
                        for comment in comments_data['entries']
                    )
                else:
                    logger.warning(f"Failed to fetch comments for merge proposal {merge_proposal.self_link}: HTTP {comments_response.status_code}")
            except Exception as e:
                logger.warning(f"Error fetching comments for merge proposal {merge_proposal.self_link}: {e}")
                
            # Skip if no dates are in range
            if not dates_in_range(dates, from_date, to_date): 
                continue

            # Generate parent item ID
            try:
                mp_id = merge_proposal.self_link.split('/')[-1]
                source_path = getattr(merge_proposal, 'source_git_path', 'unknown')
                parent_item_id = f"mp-{source_path}-{mp_id}"
            except Exception as e:
                logger.warning(f"Error generating parent_item_id for merge proposal {merge_proposal.self_link}: {e}")
                parent_item_id = f"mp-unknown-{merge_proposal.self_link.split('/')[-1]}"
            
            event_properties = extract_event_props(merge_proposal)
            
            # Calculate metrics based on comments response
            comments_count = 0
            if comments_response and comments_response.status_code == 200 and comments_data:
                comments_count = comments_data['total_size']
                
            metrics = {
                'comments_count': comments_count,
            }

            # Process merge proposal creation event
            if date_in_range(merge_proposal.date_created, from_date, to_date):
                registrant_id = member
                if hasattr(merge_proposal, 'registrant_link') and merge_proposal.registrant_link:
                    try:
                        registrant_id = merge_proposal.registrant_link.split('~')[-1]
                    except Exception:
                        pass
                        
                events_batch.append({
                    'parent_item_id':       parent_item_id,
                    'event_id':             f"{parent_item_id}-c",
                    'relation_type':        "merge_proposal_created",
                    'employee_id':          registrant_id,
                    'event_time_utc':       merge_proposal.date_created.isoformat(),
                    'time_zone':            time_zone,
                    'relation_properties':  {},
                    'event_properties':     event_properties,
                    'metrics':              metrics
                })

            # Process review requested event
            if date_in_range(merge_proposal.date_review_requested, from_date, to_date):
                events_batch.append({
                    'parent_item_id':       parent_item_id,
                    'event_id':             f"{parent_item_id}-rq",
                    'relation_type':        "merge_proposal_review_requested",
                    'employee_id':          member,
                    'event_time_utc':       merge_proposal.date_review_requested.isoformat(),
                    'time_zone':            time_zone,
                    'relation_properties':  {},
                    'event_properties':     event_properties,
                    'metrics':              metrics
                })

            # Process reviewed event
            if date_in_range(merge_proposal.date_reviewed, from_date, to_date):
                reviewer_id = member
                if hasattr(merge_proposal, 'reviewer_link') and merge_proposal.reviewer_link:
                    try:
                        reviewer_id = merge_proposal.reviewer_link.split('~')[-1]
                    except Exception:
                        pass
                        
                events_batch.append({
                    'parent_item_id':       parent_item_id,
                    'event_id':             f"{parent_item_id}-r",
                    'relation_type':        "merge_proposal_reviewed",
                    'employee_id':          reviewer_id,
                    'event_time_utc':       merge_proposal.date_reviewed.isoformat(),
                    'time_zone':            time_zone,
                    'relation_properties':  extract_merge_proposal_reviewed_relation_props(merge_proposal),
                    'event_properties':     event_properties,
                    'metrics':              metrics
                })

            # Process merged event
            if date_in_range(merge_proposal.date_merged, from_date, to_date):
                merge_reporter_id = member
                if hasattr(merge_proposal, 'merge_reporter_link') and merge_proposal.merge_reporter_link:
                    try:
                        merge_reporter_id = merge_proposal.merge_reporter_link.split('~')[-1]
                    except Exception:
                        pass
                        
                events_batch.append({
                    'parent_item_id':       parent_item_id,
                    'event_id':             f"{parent_item_id}-m",
                    'relation_type':        "merge_proposal_merged",
                    'employee_id':          merge_reporter_id,
                    'event_time_utc':       merge_proposal.date_merged.isoformat(),
                    'time_zone':            time_zone,
                    'relation_properties':  extract_merge_proposal_merged_relation_props(merge_proposal),
                    'event_properties':     event_properties,
                    'metrics':              metrics
                })

            # Process comments if available
            if comments_response and comments_response.status_code == 200 and comments_data:
                logger.debug(f"Processing {comments_data['total_size']} comments for merge proposal {parent_item_id}")
                
                for comment in comments_data['entries']:
                    try:
                        date_created = datetime.strptime(comment['date_created'], "%Y-%m-%dT%H:%M:%S.%f%z")
                        if not date_in_range(date_created, from_date, to_date):
                            continue  # Skip comments outside the date range

                        # Extract employee ID from comment author
                        comment_employee_id = member
                        if comment.get('author_link'):
                            try:
                                comment_employee_id = comment['author_link'].split('~')[-1]
                            except Exception:
                                pass

                        # Determine if this is a vote or regular comment
                        is_vote = comment.get('vote') is not None
                        comment_id = comment.get('id', 'unknown')
                        
                        events_batch.append({
                            'parent_item_id':       parent_item_id,
                            'event_id':             f"{parent_item_id}-v{comment_id}" if is_vote else f"{parent_item_id}-c{comment_id}",
                            'relation_type':        'merge_proposal_vote' if is_vote else 'merge_proposal_comment',
                            'employee_id':          comment_employee_id,
                            'event_time_utc':       date_created.isoformat(),
                            'time_zone':            time_zone,
                            'relation_properties':  extract_merge_proposal_comment_relation_props(comment),
                            'event_properties':     event_properties,
                            'metrics':              metrics
                        })
                    except Exception as e:
                        logger.warning(f"Error processing comment {comment.get('id', 'unknown')} for merge proposal {parent_item_id}: {e}")
                        continue
        
        logger.info(f"Merge proposal batch {i//batch_size + 1} produced {len(events_batch)} events")
        yield events_batch


"""Helper functions to extract properties for various merge proposal events and relations."""

def extract_event_props(merge_proposal) -> dict:
    """Extract merge proposal event properties."""
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
    """Extract merge proposal reviewed relation properties."""
    property_mappings = [
        (merge_proposal.reviewed_revid, 'reviewed_revid'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})
    return relation_properties


def extract_merge_proposal_merged_relation_props(merge_proposal) -> dict:
    """Extract merge proposal merged relation properties."""
    property_mappings = [
        (merge_proposal.merged_revision_id, 'merged_revision_id'),
        (merge_proposal.merged_revno, 'merged_revno'),
    ]
    relation_properties = {}
    relation_properties.update({key: value for value, key in property_mappings if value})
    return relation_properties


def extract_merge_proposal_comment_relation_props(comment) -> dict:
    """Extract merge proposal comment relation properties."""
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


