from typing import List
from temporalio import activity
from db.db import get_database
from models.event import Event
from models.query import IQuery


class IActivities:
    """
    Interface for Launchpad activities.
    Implement this class to define specific extraction and transformation logic.
    """
    queue_name: str

    @staticmethod
    def get_activities() -> list:
        """
        Returns a list of activities to be registered with the worker.
        """
        # Methods should be implemented in the same file as class
        # E.g.: an ETL pipeline would have methods like:
        # return [extract_data, transform_data, load_data]
        raise NotImplementedError("Subclasses must implement this method.")


# @activity.defn
# async def load_data(events: List[Event]) -> int:
#     """
#     Write event to database.

#     :param events: Events to load into the database.
#     """
#     if len(events) == 0: 
#         return 0
    
#     db = get_database()
#     return db.insert_events_batch(events)