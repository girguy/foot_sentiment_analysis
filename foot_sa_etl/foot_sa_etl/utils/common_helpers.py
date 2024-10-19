import hashlib
import pytz  # To manage time zones
from datetime import datetime
from pattern.en import sentiment


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


def get_sentiment(polarity: float, threshold: float) -> str:
    """
    Determines the sentiment based on the polarity value and a threshold.
    
    :param polarity: The polarity score, a float in the range [-1, 1].
    :param threshold: A threshold value for distinguishing between neutral and other sentiments.
    :return: A string representing the sentiment ('negative', 'positive', or 'neutral').
    """

    # Check if the polarity is in the negative range: [-1, 0 - threshold[
    if (polarity >= -1) and (polarity < (0 - threshold)):
        return 'negative'
    
    # Check if the polarity is in the positive range: [0 + threshold, 1]
    elif (polarity <= 1) and (polarity > (0 + threshold)):
        return 'positive'
    
    # If the polarity is in the neutral range: [-threshold, threshold[
    else:
        return 'neutral'


def is_subjectivity(subjectivity: float) -> str:
    """
    Determines whether the given subjectivity score corresponds to a subjective or objective sentiment.
    
    :param subjectivity: The subjectivity score, a float in the range [0, 1].
    :return: A string representing whether the sentiment is 'subjective' or 'objective'.
    """

    # If the subjectivity score is in the range [0.5, 1], the sentiment is subjective
    if (subjectivity >= 0.5) and (subjectivity <= 1):
        return 'subjective'
    
    # If the subjectivity score is in the range [0, 0.5[, the sentiment is objective
    else:
        return 'objective'


def extract_sentiment(reaction_id: str, content: str, threshold_polarity: float) -> tuple:
    """
    Extracts sentiment and subjectivity from the content and returns detailed sentiment analysis.
    
    :param reaction_id: The unique identifier for the reaction.
    :param content: The content or text from which sentiment and subjectivity are to be extracted.
    :param threshold_polarity: The threshold used to classify sentiment as neutral.
    :return: A tuple containing the following:
        - reaction_id (str): The ID of the reaction.
        - polarity_value (float): The raw polarity score, a float in the range [-1, 1].
        - polarity (str): The classified sentiment ('negative', 'positive', or 'neutral').
        - subjectivity_value (float): The raw subjectivity score, a float in the range [0, 1].
        - subjectivity (str): The classified subjectivity ('subjective' or 'objective').
    """
    
    # Extract the sentiment analysis results from the content (polarity and subjectivity)
    res = sentiment(content)
    polarity_value = res[0]  # Polarity value is in the range [-1, 1]
    subjectivity_value = res[1]  # Subjectivity value is in the range [0, 1]

    # Classify the polarity based on the polarity value and threshold
    polarity = get_sentiment(polarity_value, threshold_polarity)

    # Classify the subjectivity based on the subjectivity score
    subjectivity = is_subjectivity(subjectivity_value)

    # Return a tuple with the reaction_id and the sentiment analysis details
    return (reaction_id, polarity_value, polarity, subjectivity_value, subjectivity)
