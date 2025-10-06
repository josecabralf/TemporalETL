import asyncio
import sys

from external.temporal.client import TemporalClient

from models.queuer.flow import QueuerFlow


def validate_schedule_args(day_of_week: int, hour: int):
    errors = []
    if day_of_week < 0 or day_of_week > 6:
        errors.append("day_of_week must be between 0 (Sunday) and 6 (Saturday).")
    if hour < 0 or hour > 23:
        errors.append("hour must be between 0 and 23.")
    if errors:
        for error in errors:
            print(f"Error: {error}")
        sys.exit(1)


helper_text = """Usage: python schedule.py <source> [day_of_week] [hour]
    <source>:        The source to be processed by the queuer.
    [day_of_week]:   The day of the week to run the schedule (0-6, default: 0 = Sunday).
    [hour]:          The hour of the day to run the schedule (0-23, default: 0 = Midnight).
"""

if __name__ == "__main__":
    # Grab the source parameter from command line arguments
    if len(sys.argv) < 2 or any(arg in sys.argv for arg in ["-h", "--help"]):
        print(helper_text)
        sys.exit(0)

    try:
        source = sys.argv[1]
        day_of_week = 0  # Default to Sunday
        hour = 0  # Default to midnight
        if len(sys.argv) > 3:
            day_of_week = int(sys.argv[2])
            hour = int(sys.argv[3])

        validate_schedule_args(day_of_week, hour)
        asyncio.run(
            TemporalClient.create_schedule(QueuerFlow, source, day_of_week, hour)
        )
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
