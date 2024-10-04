import hashlib
import pytz  # To manage time zones
from datetime import datetime


def get_current_datetime(timezone: str = 'Europe/Vienna') -> str:
    """
    Returns the current date and time in the specified timezone.

    :param timezone: Timezone name, default is 'Europe/Vienna'
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


def create_blob_name(timestamp: str) -> str:
    """
    Creates a blob name in the format 'epl_news_YYYY_MM_DD' based on the given timestamp string.

    :param timestamp: A string representing the date and time (e.g., '2024-10-03 10:46:15')
    :return: A formatted string in the format 'epl_news_YYYY_MM_DD'
    """
    # Extract the date part from the timestamp
    date_part = timestamp.split(' ')[0]

    # Replace the hyphens with underscores
    formatted_date = date_part.replace('-', '_')

    # Return the final formatted blob name
    return f"epl_news_{formatted_date}"