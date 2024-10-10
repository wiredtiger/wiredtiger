import enum
from typing import Union, Any, Optional, TYPE_CHECKING, cast, Iterator, TypeAlias, Generator, Iterable, Callable, NamedTuple, TypedDict, Literal
from dataclasses import dataclass
from copy import deepcopy
import regex

from glob import glob

re_token = r'''(?(DEFINE)(?<TOKEN>
    (?> (?: \# | \/\/ ) (?: [^\\\n] | \\. )*+ \n) |
    (?> \/\* (?: [^*] | \*[^\/] )*+ \*\/ ) |
    (?> " (?> [^\\"] | \\. )* " ) |
    (?> ' (?> [^\\'] | \\. )* ' ) |
    (?> \{ (?&TOKEN)*+ \} ) |
    (?> \( (?&TOKEN)*+ \) ) |
    (?> \[ (?&TOKEN)*+ \] ) |
    (?>\n) |
    [\r\t ]++ |
    (?>\\.) |
    (?> , | ; | \? | : |
        ! | \~ |
        <<= | >>= |
        \+\+ | \-\- | \-> | \+\+ | \-\- | << | >> | <= | >= | == | != |
        \&\& | \|\| | \+= | \-= | \*= | /= | %= | \&= | \^= | \|= |
        \. | \+ | \- | \* | \& | / | % | \+ | \- | < | > |
        \& | \^ | \| | = |
        \@ # invalid charachter
    ) |
    \w++
))''' # /nxs;

regex.DEFAULT_VERSION = regex.RegexFlag.VERSION1
re_flags = regex.RegexFlag.VERSION1 | regex.RegexFlag.DOTALL | regex.RegexFlag.VERBOSE # | regex.RegexFlag.POSIX

reg_token = regex.compile(r"(?&TOKEN)"+re_token, re_flags)
reg_token_r = regex.compile(r"(?&TOKEN)"+re_token, re_flags | regex.RegexFlag.REVERSE)

Range: TypeAlias = tuple[int, int]
def rangeShift(rng: Range, offset: int) -> Range:
    return (rng[0]+offset, rng[1]+offset)

reg_identifier = regex.compile(r"^[a-zA-Z_]\w++$", re_flags)
reg_type = regex.compile(r"^[\w\[\]\(\)\*\, ]++$", re_flags)

c_type_keywords = ["const", "volatile", "restrict", "static", "extern", "auto", "register", "struct", "union", "enum"]
c_statement_keywords = [
    "case", "continue", "default", "do", "else", "for", "goto", "if",
    "return", "switch", "while",
]
reg_statement_keyword = regex.compile(r"^(?:" + "|".join(c_statement_keywords) + r")$", re_flags)

c_types = ["void", "char", "short", "int", "long", "float", "double", "signed", "unsigned",
           "bool", "size_t", "ssize_t", "ptrdiff_t", "intptr_t", "uintptr_t", "int8_t", "int16_t", "int32_t", "int64_t",
           "uint8_t", "uint16_t", "uint32_t", "uint64_t", "int_least8_t", "int_least16_t", "int_least32_t", "int_least64_t",
           "uint_least8_t", "uint_least16_t", "uint_least32_t", "uint_least64_t", "int_fast8_t", "int_fast16_t", "int_fast32_t",
           "int_fast64_t", "uint_fast8_t", "uint_fast16_t", "uint_fast32_t", "uint_fast64_t", "intmax_t", "uintmax_t",
           "wchar_t", "char16_t", "char32_t",
           "__int128", "__uint128", "__float80", "__float128", "__float16", "__float32", "__float64", "__float128",
           "__int64", "__uint64", "__int32", "__uint32", "__int16", "__uint16", "__int8", "__uint8",]

ignore_type_keywords = ["__attribute__", "__extension__", "__restrict__", "__restrict", "__inline__", "__inline", "__asm__", "__asm", "inline", "restrict",
    "WT_GCC_FUNC_DECL_ATTRIBUTE", "WT_GCC_FUNC_ATTRIBUTE", "WT_INLINE", "wt_shared",
    "WT_STAT_COMPR_RATIO_READ_HIST_INCR_FUNC", "WT_STAT_COMPR_RATIO_WRITE_HIST_INCR_FUNC", "WT_STAT_USECS_HIST_INCR_FUNC"
    "WT_ATOMIC_CAS_FUNC", "WT_ATOMIC_FUNC",
    "WT_CURDUMP_PASS",
    ]

# c_operators_1c_all_all = ["!", "%", "&", "*", "+", "-", ".", "/", ":", "<", "=", ">", "?", "^", "|", "~"]
c_operators_1c_all = ["=", "+", "-", "%", "&", "|", "^", "~", ".", "?", ":", "*", ">", "<"] # "/", "!", ",", ";", "~"
c_operators_1c_no_star = ["=", "+", "-", "%", "&", "|", "^", "~", ".", "?", ":", ">", "<"]
c_operators_1c_no_dash = ["=", "+", "%", "&", "|", "^", "~", ".", "?", ":", "*", ">", "<"]
c_ops_all = (
    "<<=", ">>=",
    "++", "--", "->", "++", "--", "<<", ">>", "<=", ">=", "==", "!=", "&&", "||", "+=", "-=", "*=", "/=", "%=", "&=", "^=", "|=",
    ".", "+", "-", "!", "~", "*", "&", "*", "/", "%", "+", "-", "<", ">", "&", "^", "|", "?", ":", "=",
    ",", ";",
    #",", "sizeof", "_Alignof", "(",")", "[","]", "(type)", ";",
)

reg_member_access = regex.compile(r"^\.|->", re_flags)

# reg_c_operators = regex.compile(r"(?:" + "|".join([regex.escape(op) for op in c_operators_1c_no_star]) + r")", re_flags)

def file_content(fname: str) -> str:
    with open(fname) as file:
        return file.read()


reg_word_char = regex.compile(r"\w", re_flags)

_multithreading_initialized = False

def init_multithreading():
    global _multithreading_initialized
    if _multithreading_initialized:
        return
    _multithreading_initialized = True
    import multiprocessing
    multiprocessing.set_start_method('fork')  # 'fork' is faster than 'spawn'
