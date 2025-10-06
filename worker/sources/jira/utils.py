from datetime import datetime
import re
from typing import Tuple, List
from collections import OrderedDict

import atlas_doc_parser.model as adp


# Override the method globally
def custom_mention_to_markdown(
    self: adp.NodeMention, ignore_error: bool = False
) -> str:
    if not self.attrs or not self.attrs.id:
        return "@mention"
    template = "@{{{{{text}}}}}"
    return template.format(text=self.attrs.id)


adp.NodeMention.to_markdown = custom_mention_to_markdown


class JiraUtils:
    @staticmethod
    def parse_jira_datetime(date_str: str) -> datetime:
        """Parse Jira datetime string to datetime object."""
        possible_formats = [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
        ]
        for fmt in possible_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Date string '{date_str}' is not in a recognized format.")

    @staticmethod
    def is_system_account_mail(email: str) -> bool:
        """Check if the email belongs to a known system account."""
        return email in {
            ...
        }

    @staticmethod
    def is_system_account_id(id: str) -> bool:
        """Check if the email belongs to a known system account."""
        return id in {
            ...
        }

    @classmethod
    def parse_adf(cls, adf: dict) -> Tuple[str, List[str]]:
        """
        Parses Atlassian Document Format (ADF) to markdown
        and extracts mentions.
        Returns a tuple of (markdown string, list of mentioned user IDs).
        """
        if not adf:
            return "", []

        try:
            root = adp.NodeDoc.from_dict(adf)
            content, mentions = cls.extract_mentions(
                root.to_markdown(ignore_error=True)
            )
            content = re.sub(r"\n{2,}", "\n", content).strip()
            return content, mentions
        except Exception as e:
            # Handle unknown ADF node types (like 'embedCard')
            return f"ERROR while extracting. Message: {str(e)}", []

    @staticmethod
    def extract_mentions(text: str, pattern=r"@{{(.*?)}}") -> Tuple[str, List[str]]:
        """
        Extracts mentions from markdown and returns a tuple of
        (markdown with placeholders, list of mentioned user IDs).
        """
        mention_indices = OrderedDict()

        def replacer(match):
            mention = match.group(1)
            if mention not in mention_indices:
                mention_indices[mention] = len(mention_indices)
            return f"@{{{mention_indices[mention]}}}"

        new_text = re.sub(pattern, replacer, text)
        return new_text, list(mention_indices.keys())
