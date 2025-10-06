from json.decoder import JSONDecodeError

from models.logger import logger


class Person:
    def __init__(self, name: str, timezone: str, link: str):
        self.name = name
        self.timezone = timezone
        self.link = link


def get_user(member: str, launchpad):
    try:
        return launchpad.people[member.lower()]  # type: ignore
    except KeyError:
        logger.warning("User '%s' not found in Launchpad.", member)
        return None
    except JSONDecodeError:
        logger.warning("Malformed username '%s'.", member)
        return None
    except Exception as e:
        raise ValueError("Error fetching member %s: %s", member, e)
