from typing import Any, Dict, Iterator, List

from models.logger import logger
from models.etl.input import ETLInput
from models.queuer.inputs_strategy import transform_inputs_method

from sources.jira.query import JiraQuery


@transform_inputs_method("jira")
def transform_jira_inputs_iterator(params: Dict[str, Any]) -> Iterator[ETLInput]:
    """Transform jira params into a List of ETLInput objects."""
    issues = params.get("issues")
    if not issues:
        logger.warning("No issues provided for jira inputs.")
        return

    if not isinstance(issues, list):
        raise ValueError("Issues must be a list.")

    date_start = params.get("date_start", "")
    date_end = params.get("date_end", "")
    if not date_end and not date_start:
        raise ValueError("Both date_end and date_start must be provided.")

    for issue_id in issues:
        query = JiraQuery(
            issue_id=issue_id,
            date_start=date_start,
            date_end=date_end,
            source_kind_id=params["source_kind_id"],
            event_type="issues",
        )
        yield ETLInput(
            job_id=f"jira:{issue_id}_{date_start}-{date_end}",
            query_type=params["source_kind_id"],
            args=query.to_dict()
        )
