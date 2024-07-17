import re


def consolidate_text(text):
    consolidated = text.replace('\r', ' ').replace('\n', ' ')
    consolidated = re.sub(' +', ' ', consolidated)
    return consolidated
