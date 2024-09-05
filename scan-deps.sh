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
    if (@structstackindent) {
        my $i = index($_, "-");
        if ($i < 0) {
            @structstackindent = (); @structstack = ();
        } else {
            while (@structstackindent && $structstackindent[-1] >= $i) {
                pop @structstackindent; pop @structstack;
            }
        }
    }
    if (/^(.*?)-RecordDecl .*? (struct|union) (?:(\w+) )?definition/) {
        $h{"$file: -: DEF($2): $3"} = 1 if $3;
        push @structstackindent, length $1;
        push @structstack, ($3 || ($structstack[-1] || "-")."-unnamed".(++$unnamed));
    }
    if (@structstack && /-FieldDecl .* referenced (\w++)/) {
        $h{"$file: ".($func || "-").": FIELD: $structstack[-1]->$1"} = 1;
    }
    $h{"$file: -: DEF(type): $1 => ".(($type=$2) =~ s/\x27:\x27/ => /r =~ s/\s++\*++( =|$)/$1/gr)} = 1 if /-TypedefDecl .*? referenced (\w++) \x27(.*?)\x27[^\x27]*$/;

    next if !$func;

    # $h{"$file: $func: TYPE: ".(($type=$1) =~ s/\x27:\x27/ => /r =~ s/\s++\*++( =|$)/$1/gr)}=1 if /ImplicitCastExpr.*?\x27(.*?)\x27[^\x27]*$/;
    $h{"$file: $func: FUNC: $1"}=1 if /DeclRefExpr .*? Function 0x\w++ \x27([^\x27]++)\x27/;
    if (/MemberExpr.*? (?:->|\.)(\w++) 0x/) {
        $member = $member ? "$1.$member" : $1;
        next;
    }
    if ($member) {
        if (/(?:ImplicitCastExpr|ParenExpr|DeclRefExpr).*?\x27(.*?)\x27/) {
            $type = $1;
            $h{"$file: $func: MEMBER: ".($type =~ s/(struct|union|const|\*)//gr =~ s/(^\s++)|(\s++$)//gr)."->$member"} = 1;
        }
        $member = undef;
    }
    END { say for sort keys %h }
' # `find build/CMakeFiles/wt_objs.dir -name \*.ast`

