#!/bin/bash

cd .
cd `git rev-parse --show-toplevel`
export LC_ALL=C

find build/CMakeFiles/wt_objs.dir -name \*.i |
xargs -I'{}' -n1 -P8 -- bash -c 'f() { [[ "$1.ast" -nt "$1" ]] || clang -cc1 -ast-dump $1 > $1.ast; echo $1.ast; }; f {}' 2>/dev/null |
xargs -n1 cat |
perl -nE '
    our (%typedef);
    sub untypedef($) {
        my $type = shift;
        $type = $typedef{$type} while $typedef{$type};
        return $type;
    }
    sub file2mod($) {
        local $_ = shift;
        my $wd = $ENV{PWD};
        return  !s{^$wd/}{} ? "" :                              # not in $wd
                s{^src/include/}{} ? lc(s{(_inline|^(wt_internal|extern|wiredtiger_ext))?\.h$}{}r) : # in src/include/...
                s{^src/}{} ? lc(s{/.*$}{}r) :                   # in src/...
                "";
    }
    sub identifier2mod($$) {   # name, default
        local $_ = shift;
        return s/^[_]*wt(?:i?)_// ? lc(s{_.*$}{}r) : $_[0];
    }
    sub clean_type($) {
        return $_[0] =~
            s/\x27:\x27/ => /r =~
            s/\s++[\*\[\]]++( =|$)/$1/gr =~
            s/^((?:struct|union|enum|const|volatile|restrict)\s*)+//r;
    }
    sub identifier2access($) {
        local $_ = shift;
        s/^((?:struct|union|enum|const|volatile|restrict)\s*)+//;
        return !/^[_]*wt([ip])?_/ ? "" : $1 ? "private" : "public";
    }

    if (m{^.-\w.*? \<(\/.*?):\d++:\d++, [^>]++\>}) {
        $file = $1;
        $module = file2mod($file);
    } elsif (/\~<<invalid sloc>>/ || /^TranslationUnitDecl/) {
        $module = $file = undef;
    }

    next if !$file;

# print "$file [$module] $_";

    if (/-TypedefDecl .*? referenced (\w++) \x27([^\x27]*+)\x27/) {
        my $newtype = $1;
        my $oldtype = clean_type($2);
        $typedef{$newtype} = $oldtype if $oldtype && $newtype && $oldtype ne $newtype;
# say "TYPEDEF: $newtype $oldtype";
        if ($module) {
            my ($newaccess, $oldaccess) = (identifier2access($newtype), identifier2access($oldtype));
            my ($newmod, $oldmod) = (identifier2mod($newtype, $module), identifier2mod($oldtype, $module));
# say "TYPEDEF: $newtype [$newmod] : $newaccess = $oldtype [$oldmod] : $oldaccess";
            if ($oldaccess eq "private" && $newmod ne $oldmod) {
                $h{"ERROR: $file [$module]: $func: TYPE $newtype: invalid access to type $oldtype of [$oldmod]"} = 1;
            }
            $type2mod{$2} = $module;
        }
    }

    next if !$file || !$module;

    if (/^.-/) {
        if (m{^.-FunctionDecl.*?\<(.*?):\d++:\d++, line:\d++:\d++\> line:\d++:\d++ (?:used )?(\w++)}) {
            $func = $2;
# say "FUNC: $func [".identifier2mod($func, $module)."] : ".identifier2access($func);
            if (identifier2access($func) eq "private") {
                $func2mod{$func} = identifier2mod($func, $module);
            }
        } else {
            $func = undef;
        }
    }

    if (@structstack) {
        my $i = index($_, "-");
        if ($i < 0) {
            @structstack = ();
        } else {
            while (@structstack && $structstack[-1]{indent} >= $i) {
                pop @structstack;
            }
        }
    }
    if (/^(?<indent>.*?)-RecordDecl .*? \<(.*?):(?<line>\d++):(?<col>\d++), [^>]++\> .*? (?<what>struct|union) (?:(?<type>\w+) )?definition/) {
        my $type = $+{type} || "(unnamed $+{what} at $file:$+{line}:$+{col})";
        my ($what, $indent) = ($+{what}, length $+{indent});
        # struct __wt_block::(unnamed at /Users/y.ershov/src/wt-cc/src/include/block.h:267:5)
        push @structstack, {
            type => $type,
            indent => $indent,
            module => identifier2mod($type, !@structstack ? $module : $structstack[-1]{module}),
            access => ($access = identifier2access($type))};
# say "DEF($what): $type [$module] : $access";
        if ($access eq "private") {
            $type2mod{$type} = $module;
        }
    }
    if (@structstack && /-FieldDecl .* (?:referenced )?(\w++) \x27(.*?)\x27/) {
        my ($name, $type) = ($1, $2);
        my $namemod = identifier2mod($name, $structstack[-1]{module});
        my $nameaccess = identifier2access($name);
        my $type = untypedef(clean_type($type));
        my $typemod = $type2mod{$type};
# say "FIELD: $structstack[-1]{type}.$name [$namemod] : $nameaccess of type $type : $typemod";
        if ($typemod && $typemod ne $namemod) {
            $h{"ERROR: $file [$module]: $func: FIELD $name: invalid access to type $type of [$typemod]"} = 1;
        }
        if ($nameaccess eq "private") {
            $field2mod{$structstack[-1]{type}}{$name} = $namemod;
# say "  ... registered as private: type $structstack[-1]{type} field $name";
        }
    }

    next if !$func;

    if (/DeclRefExpr .*? Function 0x\w++ \x27([^\x27]++)\x27/) {
        my $target = $1;
# say "CALL: $func [$module] => $target [$func2mod{$target}]";
        if ($func2mod{$target}) {
            my $targetmod = $func2mod{$target};
            if ($module ne $targetmod) {
                $h{"ERROR: $file [$module]: $func: FUNC invalid call to $target of [$targetmod]"} = 1;
            }
        }
    }

    if ($member) {
        if (/(?:ImplicitCastExpr|ParenExpr|DeclRefExpr|MemberExpr).*?\x27([^\x27]*+)\x27/) {
            my $type = $1;
            my $type = untypedef(clean_type($type));
            my $typemod = $type2mod{$type};
            my $membermod = ($field2mod{$type} && $field2mod{$type}{$member}) || "";
# say "MEMBER: [$module] $type [$typemod] -> $member [$membermod]";
            if ($typemod && $typemod ne $module) {
                $h{"ERROR: $file [$module]: $func: FIELD $name: invalid access to type $type of [$typemod] (member $member)"} = 1;
            }
            if ($membermod && $membermod ne $module) {
                $h{"ERROR: $file [$module]: $func: FIELD $name: invalid access to field ${type}::$member of [$membermod]"} = 1;
            }
        }
        $member = undef;
    }

    if (/MemberExpr.*? \x27(.*?)\x27 [^\x27]*? (?:->|\.)(\w++) 0x/) {
        $member = $2;
# say "... MEMBER: $member";
    }

    END { say for sort keys %h }
' > errors.txt # `find build/CMakeFiles/wt_objs.dir -name \*.ast`

