from launchpad.query import LaunchpadQuery

from models.activities import IActivities
from models.event import Event

from temporalio import activity

from launchpadlib.launchpad import Launchpad


class MergeProposalActivities(IActivities):
    queue_name = "launchpad-merges-task-queue"