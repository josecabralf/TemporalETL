from launchpad.query import LaunchpadQuery

from models.activities import IActivities
from models.event import Event

from temporalio import activity

from launchpadlib.launchpad import Launchpad


class BugsActivities(IActivities):
    queue_name = "launchpad-bugs-task-queue"