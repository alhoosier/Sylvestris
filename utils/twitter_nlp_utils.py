# Standard Python libraries
import re
from textblob import TextBlob

# Private regex string containing logic for capturing specific emoticons within a tweet's text
_EMOTICONS_REGEX_STR = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

# Private regex string containing logic to manually tokenize a tweet's text
_REGEX_STR = [
    _EMOTICONS_REGEX_STR,
    r'<[^>]+>',  # HTML tags
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",  # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
    r'(?:[\w_]+)',  # other words
    r'(?:\S)'  # anything else
]

# Regex compilers for capturing emoticons and tokenizing tweets
emoticon_re = re.compile(r'^'+_EMOTICONS_REGEX_STR+'$', re.VERBOSE | re.IGNORECASE)
tokens_re = re.compile(r'('+'|'.join(_REGEX_STR)+')', re.VERBOSE | re.IGNORECASE)


def _clean_tweet(tweet):
    """
    Utility function to clean tweet text by removing links, special character using simple regex statements

    :param tweet: tweet text to clean
    :return cleaned tweet text
    """
    return ' '.join(re.sub('(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+: / {2}/ \S+)', ' ', tweet).split())


def get_tweet_sentiment(tweet):
    """
    Utility function to classify sentiment of passed tweet using textblob's sentiment method

    :param tweet: tweet text to get sentiment from
    :return sentiment estimate based on textblob's sentiment method
    """
    # create TextBlob object of passed tweet text
    analysis = TextBlob(_clean_tweet(tweet))
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


def _tokenize(tweet):
    """
    Private function to use tweet tokenizer regex to tokenize string

    :param tweet: tweet text to tokenize
    :return: returns list of tokens returned by tweet tokenizer regex
    """
    return tokens_re.findall(tweet)


def tokenize_tweet_text(tweet, lowercase=False):
    """
    Tokenize tweet text using manual regex logic

    :param tweet: tweet text
    :param lowercase: convert tokens to lowercase (except emoticons) if set to True
    :return: list of tokens from tweet text
    """
    tokens = _tokenize(tweet)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens
