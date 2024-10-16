from dataclasses import dataclass

from .workspace import *
from .macro import *

def c_string_escape(txt: str) -> str:
    return txt.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t").replace("\"", "\\\"")

def _D2M(val: 'Definition') -> MacroParts: # type: ignore[name-defined] # circular dependency for Definition
    return cast(MacroParts, val.details)

# @dataclass
# class ExpandTree:
#     expansionTree: 'map[str, ExpandTree]'
#     # name: str

class MacroExpander:
    insert_list: InsertList  # sorted list by range[0] of (offset, delta)
    expand_list: list[Expansions]  # sorted list by range[0] of ExpandList
    _macros: dict[str, MacroParts]
    _cur_expand_entry: dict[str, set[str]]

    # Macro expansion.
    #  - https://en.wikipedia.org/wiki/C_preprocessor#Order_of_expansion
    #  - https://stackoverflow.com/questions/45375238/c-preprocessor-macro-expansion
    #  - https://gcc.gnu.org/onlinedocs/cpp/Argument-Prescan.html
    def expand(self, txt: str, macros: 'dict[str, Definition]',expand_const: bool = False) -> str: # type: ignore[name-defined] # circular dependency for Definition
        # TODO(later): Optimise: compose the result as a list of strings, then join at the end

        self.insert_list = []  # (offset, delta)
        self.expand_list = []
        if not macros:
            return txt
        self._macros = macros
        self._cur_expand_entry = {}

        names_re_a = [
            r"""(?> (?: \# | \/\/ ) (?: [^\\\n] | \\. )*+ \n)""",
            r"""(?> \/\* (?: [^*] | \*[^\/] )*+ \*\/ )""",
            r"""(?> " (?> [^\\"] | \\. )* " )""",
            r"""(?> ' (?> [^\\'] | \\. )* ' )""",
        ]
        kwargs = {}
        self._has_obj_like_names, self._has_fn_like_names = False, False
        obj_like_names, fn_like_names = [], []
        for k, v in self._macros.items():
            macro = _D2M(v)
            if not expand_const and macro.is_const:
                continue
            if macro.args is None:
                obj_like_names.append(k)
            else:
                fn_like_names.append(k)
        if obj_like_names:
            kwargs["names_obj"] = obj_like_names
            names_re_a.append(r"""(?P<name> \b(?:\L<names_obj>)\b )""")
            self._has_obj_like_names = True
        if fn_like_names:
            kwargs["names_func"] = fn_like_names
            names_re_a.append(r"""
                (?P<name> \b(?:\L<names_func>)\b )
                (?P<args>(?P<spc>\s*+)\((?P<list>(?&TOKEN)*+)\))""" + re_token)
            self._has_fn_like_names = True
        del obj_like_names, fn_like_names # There can be many, so free up memory

        if not self._has_obj_like_names and not self._has_fn_like_names:
            return txt

        self._names_reg = regex.compile(" | ".join(names_re_a), re_flags, **kwargs)  # type: ignore # **kwargs
        self._recurse_in_use: set[str] = set()  # recursion control
        self._owner_stack: list[str] = []       # stack of current expansion "owning" scopes
        self._expanding_stack: list[str] = []   # stack of current expansion levels
        self._expand_offset = 0

        # The difference between _owner_stack and _expanding_stack is best demonstrated by the
        # following example. Consider the following code:
        #
        #     CAT(HE, LLO)
        #
        # The topmost expansion matches the entire macro "CAT(HE, LLO)" and _expanding_stack is
        # entered into "CAT". The _owner_stack is empty because expansion of its arguments "HE" and
        # "LLO" are performed on the topmost level, not within the "CAT" macro.
        # Then, when the contents of "CAT" are expanded with arguments substituted, the _owner_stack
        # is set to "CAT" as well.

        ret = self._expand_fragment(txt)
        del self._macros, self._cur_expand_entry # delete temporaries
        return ret

    def __expand_enter(self, name: str) -> None:
        self._expanding_stack.append(name)
        parent = self._owner_stack[-1] if self._owner_stack else ""
        if parent not in self._cur_expand_entry:
            self._cur_expand_entry[parent] = set()
        self._cur_expand_entry[parent].add(name)
        DEBUG4(None, lambda: f"Expanding macro {' => '.join(self._owner_stack)} => {name}")

    def __expand_leave(self, replacement: str, match: regex.Match, base_offset) -> str:
        DEBUG5(None, f"Macro expand {self._expanding_stack}: <<<{replacement}>>>")
        self._expanding_stack.pop()
        if not self._expanding_stack:
            delta = len(replacement) - len(match[0])
            range = (match.start() + base_offset, delta, )
            self.insert_list.append(range)
            self.expand_list.append(Expansions(
                (self._expand_offset + range[0], self._expand_offset + range[0] + len(replacement)),
                self._cur_expand_entry))
            self._expand_offset += delta
            self._cur_expand_entry = {}
        return replacement

    def _expand_fragment(self, txt: str, base_offset: int = 0) -> str:
        return self._names_reg.sub(
            lambda match: self._expand_fn_like(match, base_offset + base_offset) \
                                if self._has_fn_like_names and match["args"] else \
                          self._expand_obj_like(match, base_offset + base_offset) \
                                if self._has_obj_like_names and match["name"] else \
                          match[0],
            txt)

    def _expand_obj_like(self, match: regex.Match, base_offset: int = 0) -> str:
        name = match["name"]
        self.__expand_enter(name)
        if name in self._recurse_in_use:
            return self.__expand_leave(match[0], match, base_offset)
        if not _D2M(self._macros[name]).body:
            return self.__expand_leave("", match, base_offset)

        self._recurse_in_use.add(name)
        self._owner_stack.append(name)
        # TODO: push scope
        replacement = self._expand_fragment(_D2M(self._macros[name]).body.value, base_offset)  # type: ignore # match is not None
        self._owner_stack.pop()
        self._recurse_in_use.remove(name)

        return self.__expand_leave(replacement, match, base_offset)

    def _expand_fn_like(self, match: regex.Match, base_offset: int = 0) -> str:
        name = match["name"]
        self.__expand_enter(name)
        if name in self._recurse_in_use:
            return self.__expand_leave(match[0], match, base_offset)
        macro = _D2M(self._macros[name])
        if not macro.body:
            return self.__expand_leave("", match, base_offset)

        # Parse args
        args_val: list[TokenList] = [TokenList([])]
        for token_arg in TokenList.xFromText(match["list"],
                                             base_offset=base_offset + match.start("list")):
            # if token_arg.getKind() in ["/", "#"]:
            #     continue
            if token_arg.value == ",":
                if len(args_val) < len(macro.args):  # type: ignore # macro has args
                    args_val.append(TokenList([]))
                    continue
                # Reached the required number of arguments
                if not macro.is_va_args:
                    break
                # if va_args, continue appending to the last list
            args_val[-1].append(token_arg)
        if len(args_val) < len(macro.args):  # type: ignore # macro has args
            ERROR(scope_file().locationStr(base_offset + match.start()),
                  f"macro {name}: got only {len(args_val)} arguments, expected {len(macro.args)}")   # type: ignore # macro has args
            return self.__expand_leave(match[0], match, base_offset)

        replacement = macro.body.value  # type: ignore # match is not None

        if macro.args:  # Can be an empty list
            args_dict = {k.value: Token(0, v.range(), "".join(v.strings()).strip())
                         for k, v in zip(macro.args, args_val)}

            # Calculate expanded arguments
            args_dict_expanded = {k:
                Token(0, v.range, self._expand_fragment(v.value, base_offset+v.range[0])) \
                    for k, v in args_dict.items()}

            # Replace operators # and ## and arguments
            # TODO: protect from expanding in comments and strings
            reg_macro_subst = regex.compile(r"""
                (?P<h> \#\s*+ (?P<n>\w++) ) |
                (?P<hh> (?P<n>\w++)(?>\s*+(\#\#)\s*+(?P<n>\w++))++) |
                (?P<n>\b(?:\L<names>)\b)
            """, re_flags, names=args_dict.keys())

            def _concat_hh(match: regex.Match) -> str:
                return "".join(((args_dict[name].value if name in args_dict else name) \
                        for name in match.capturesdict()["n"]))
            def _arg_c_escape(name: str) -> str:
                return '"'+c_string_escape(args_dict[name].value)+'"' if name in args_dict else '""'

            replacement = reg_macro_subst.sub(
                lambda match: _arg_c_escape(match["n"]) if match["h"] else \
                              _concat_hh(match) if match["hh"] else \
                              args_dict_expanded[match["n"]].value,
                replacement)

        # Another round of global replacement
        self._recurse_in_use.add(name)
        self._owner_stack.append(name)
        replacement = self._expand_fragment(replacement, base_offset+match.start())
        self._owner_stack.pop()
        self._recurse_in_use.remove(name)

        return self.__expand_leave(replacement, match, base_offset)
