import enum
from dataclasses import dataclass
from copy import deepcopy
import regex

from .internal import *

# Calculate line number from position
def lineno(txt: str, pos: int | None = None) -> int:
    return txt.count("\n", 0, pos) + 1

# Calculate column number from position
def linepos(txt: str, pos: int | None = None) -> int:
    off = txt.rfind("\n", 0, pos)
    if pos is None:
        pos = len(txt)
    return pos - off + 1 if off >= 0 else pos + 1

re_clean = r'''(
    (?P<s>(?>(?> (?:\#|\/\/) (?:[^\\\n]|\\.)*+ \n) |
    (?> \/\* (?:[^*]|\*[^\/])*+ \*\/ ))++) |
    ((?> " (?>[^\\"]|\\.)* " ) |
    (?> ' (?>[^\\']|\\.)* ' ))
)''' # /nxs;
reg_clean = regex.compile(re_clean, re_flags)

reg_cr = regex.compile(r"""[^\n]""", re_flags)

# Remove comments and preprocessor directives, preserving newlines and text size
def clean_text_sz(txt: str):
    return reg_clean.sub(lambda match: reg_cr.sub(" ", match[0]) if match["s"] else match[0], txt)

# Remove comments, preprocessor directives and strings, preserving newlines and text size
def clean_text_more_sz(txt: str):
    return reg_clean.sub(lambda match: reg_cr.sub(" ", match[0]), txt)

# Remove comments and preprocessor directives
def clean_text(txt: str):
    return reg_clean.sub(lambda match: " " if match["s"] else match[0], txt)

re_clean2 = r'''(
    (?P<s>(?>\s++ |
    (?> (?:\#|\/\/) (?:[^\\\n]|\\.)*+ \n) |
    (?> \/\* (?:[^*]|\*[^\/])*+ \*\/ ))++) |
    ((?> " (?>[^\\"]|\\.)* " ) |
    (?> ' (?>[^\\']|\\.)* ' ))
)''' # /nxs;
reg_clean2 = regex.compile(re_clean2, re_flags)

# Remove comments and preprocessor directives and compact spaces
def clean_text_compact(txt: str):
    return reg_clean2.sub(lambda match: " " if match["s"] else match[0], txt)
