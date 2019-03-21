from parsers import counter_update, counter_map_update
import re


_regex_token=re.compile(r"(?u)\b\w\w\w+\b")


def tokenizer(text_string, regex_chars="[^ a-zA-Z]", lower=True,
              regex_token=_regex_token):
    """tokenizes a text string into a list of words

    Parameters
    ----------
    text_string : str
        text string to be processed
    regex_chars : str
        regex pattern which characters must match to keep, the default
        is all alphabetical characters
    lower : bool or None
        whether to set all chars to lower case
    regex_token : regex pattern
        pattern for the actual tokenzing, the default breaks up by space
        defining words as 3+ characters

    Returns
    -------
    list
        of unigrams/words matching tokenization process
    """

    text_string = re.sub(regex_chars, "", text_string)
    if lower:
        text_string = text_string.lower()
    text_list = regex_token.findall(text_string)
    return text_list


def unigram_counter(text_string, term_dict, term_map=None, **token_kwds):
    """extracts the unique unigrams from a string and inserts into term_dict

    Parameters
    ----------
    text_string

    """

    tokens = tokenizer(text_string, **token_kwds)

    if term_map:
        term_dict = counter_map_update(tokens, term_dict, term_map)
    else:
        term_dict = counter_update(tokens, term_dict)

    return term_dict
