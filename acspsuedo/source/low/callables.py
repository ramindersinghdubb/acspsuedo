"""
Small functions to support single-purpose tasks.
"""
import unicodedata
import typing as t

def str_replacement(input_str: str, replacement_dict: t.Dict[t.Any, t.Any]) -> str:
    """
    Replace substrings in a string using a replacement dictionary.
    
    Keys in this replacement dictionary are the strings to be
    replaced, while their corresponding values are the replacement
    strings.
    """

    output_str = input_str
    for old_val, new_val in replacement_dict.items():
        output_str = output_str.replace(old_val, new_val)
    return output_str


def remove_accents(input_str: str) -> str:
    """
    Return the non-accented ASCII string for the inputed string.
    """
    
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode('ASCII')