#!/bin/bash

cd .
cd `git rev-parse --show-toplevel`
export LC_ALL=C

find build/CMakeFiles/wt_objs.dir -name \*.i |
xargs -I'{}' -n1 -P8 -- bash -c 'f() { [[ "$1.ast" -nt "$1" ]] || clang -cc1 -ast-dump $1 > $1.ast; echo $1.ast; }; f {}' 2>/dev/null |
xargs -n1 cat |
perl -nE '
    if (m{^.-\w.*? \<(\/.*?):\d++:\d++, [^>]++\>}) {
        $file = $1;
    } elsif (/\~<<invalid sloc>>/) {
        $file = undef;
    }
    next if !$file;
    if (/^.-/) {
        if (m{^.-FunctionDecl.*?\<(.*?):\d++:\d++, line:\d++:\d++\> line:\d++:\d++ (?:used )?(\w++)}) {
            $func = $2;
            $h{"$file: -: DEF(func): $func"} = 1;
        } else {
            $func = undef;
        }
    }
    $h{"$file: -: DEF($1): $2"} = 1 if /-RecordDecl .*? (struct|union) (\w+) definition/;
    $h{"$file: -: DEF(type): $1 => ".(($type=$2) =~ s/\x27:\x27/ => /r =~ s/\s++\*++( =|$)/$1/gr)} = 1 if /-TypedefDecl .*? referenced (\w+) \x27(.*?)\x27[^\x27]*$/;

    next if !$func;

    # $h{"$file: $func: TYPE: ".(($type=$1) =~ s/\x27:\x27/ => /r =~ s/\s++\*++( =|$)/$1/gr)}=1 if /ImplicitCastExpr.*?\x27(.*?)\x27[^\x27]*$/;
    $h{"$file: $func: FUNC: $1"}=1 if /DeclRefExpr .*? Function 0x\w++ \x27([^\x27]++)\x27/;
    if (/MemberExpr.*?(->\w++)/) {
        $member = $1;
        next;
    }
    if ($member) {
        if (/(?:ImplicitCastExpr|ParenExpr).*?\x27(.*?)\x27/) {
            $type = $1;
            $h{"$file: $func: MEMBER: ".($type =~ s/(struct|union|const|\*)//gr =~ s/(^\s++)|(\s++$)//gr).$member} = 1;
        }
        $member = undef;
    }
    END { say for sort keys %h }
' # `find build/CMakeFiles/wt_objs.dir -name \*.ast`

