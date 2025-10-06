from datetime import datetime


class JiraUtils:
    @staticmethod
    def parse_jira_datetime(date_str: str) -> datetime:
        """Parse Jira datetime string to datetime object."""
        possible_formats = [
            "%Y-%m-%d %H:%M:%S.%f %Z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        for fmt in possible_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Date string '{date_str}' is not in a recognized format.")
