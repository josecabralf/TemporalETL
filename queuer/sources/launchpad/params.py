from typing import Dict, Any

from external.salesforce.client import SalesforceClient

from models.date_utils import get_tomorrow_date, get_last_week_date
from models.queuer.input import QueuerInput
from models.queuer.params_strategy import params_method


@params_method("launchpad")
def extract_launchpad_params(input: QueuerInput) -> Dict[str, Any]:
    """Returns the parameters needed for the launchpad queuer."""

    # Default dates: END=today - START=today-1 week
    date_end = input.date_end or get_tomorrow_date()  # date + 1 day
    date_start = input.date_start or get_last_week_date()  # date - 8 days

    return {
        "source_kind_id": "launchpad",
        "date_start": date_start,
        "date_end": date_end,
        "members": SalesforceClient.get_launchpad_ids(),
    }
