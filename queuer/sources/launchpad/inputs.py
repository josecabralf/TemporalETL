from typing import Dict, Iterator

from models.logger import logger
from models.etl.input import ETLInput
from models.queuer.inputs_strategy import transform_inputs_method

from sources.launchpad.query import LaunchpadQuery


@transform_inputs_method("launchpad")
def transform_launchpad_inputs_iterator(params: Dict[str, str]) -> Iterator[ETLInput]:
    """Transform launchpad params into a List of ETLInput objects."""
    members = params.get("members")
    if not params.get("members"):
        logger.warning("No members provided for launchpad inputs.")
        return

    if not isinstance(members, list):
        raise ValueError("Members must be a list.")

    events = ["bugs", "merge_proposals", "questions"]

    date_end = params.get("date_end", "")
    date_start = params.get("date_start", "")
    if not date_end and not date_start:
        raise ValueError("Both date_end and date_start must be provided.")

    for member in params["members"]:
        if not member:
            logger.warning("Empty member found in members list, skipping.")
            continue
        for event in events:
            # Instance a LaunchpadQuery to ensure we have the correct parameters for args
            query = LaunchpadQuery(
                member=member,
                date_start=date_start,
                date_end=date_end,
                source_kind_id=params["source_kind_id"],
                event_type=event,
            )
            yield ETLInput(
                job_id=f"launchpad-{event}:{member}_{date_start}-{date_end}",
                query_type=params["source_kind_id"],
                args=query.to_dict()
            )
