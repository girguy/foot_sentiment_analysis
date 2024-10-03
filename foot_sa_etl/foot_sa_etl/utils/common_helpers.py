import hashlib
import pytz  # To manage time zones
from datetime import datetime


def get_current_datetime(timezone: str = 'UTC') -> str:
    """
    Returns the current date and time in the specified timezone.

    :param timezone: Timezone name, default is 'UTC'
    :return: Current date and time as a formatted string
    """
    current_datetime = datetime.now(pytz.timezone(timezone))
    return current_datetime.strftime('%Y-%m-%d %H:%M:%S')


def generate_hash(content: str) -> str:
    """
    Generates a SHA-256 hash from the given content string.

    :param content: String to hash
    :return: SHA-256 hash of the input content
    """
    try:
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    except Exception as e:
        print(f"Error generating hash: {e}")
        raise
