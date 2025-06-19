from launchpad.query import LaunchpadQuery

from models.activities import IActivities
from models.event import Event

from temporalio import activity

from launchpadlib.launchpad import Launchpad


class QuestionAnswerActivities(IActivities):
    queue_name = "launchpad-questions-task-queue"