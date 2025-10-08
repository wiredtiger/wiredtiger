#!/usr/bin/env python3

import string
import wiredtiger

from enum import Enum

class OpType(Enum):
    ADD = 1
    REMOVE = 2
    REPLACE = 3

def mkstring(r, size, repeat_size, valuefmt):
    choices = string.ascii_letters + string.digits
    if valuefmt == 'S':
        pattern = ''.join(r.choice(choices) for _ in range(repeat_size))
    elif valuefmt == 'u':
        pattern = b''.join(bytes([r.choice(choices.encode())]) for _ in range(repeat_size))
    else:
        raise ValueError(f"unsupported value fmt {valuefmt}")
    return (pattern * ((size + repeat_size - 1) // repeat_size))[:size]

def create_mods(rand, oldsz, repeatsz, nmod, maxdiff, valuefmt, oldv=None):
    if oldv == None:
        oldv = mkstring(rand, oldsz, repeatsz, valuefmt)

    offsets = sorted(rand.sample(range(oldsz), nmod))
    modsizes = sorted(rand.sample(range(maxdiff), nmod + 1))
    lengths = [modsizes[i+1] - modsizes[i] for i in range(nmod)]
    modtypes = [rand.choice((OpType.ADD, OpType.REMOVE, OpType.REPLACE)) for _ in range(nmod)]

    orig = oldv
    newv = '' if valuefmt == 'S' else b''
    for i in range(1, nmod):
        if offsets[i] - offsets[i - 1] < maxdiff:
            continue
        newv += orig[:(offsets[i]-offsets[i-1])]
        orig = orig[(offsets[i]-offsets[i-1]):]
        if modtypes[i] == OpType.ADD:
            newv += mkstring(rand, lengths[i], rand.randint(1, lengths[i]), valuefmt)
        elif modtypes[i] == OpType.REMOVE:
            orig = orig[lengths[i]:]
        elif modtypes[i] == OpType.REPLACE:
            newv += mkstring(rand, lengths[i], rand.randint(1, lengths[i]), valuefmt)
            orig = orig[lengths[i]:]
    newv += orig

    try:
        mods = wiredtiger.wiredtiger_calc_modify(None, oldv, newv, max(maxdiff, nmod * 64), nmod)
    except wiredtiger.WiredTigerError:
        # When the data repeats, the algorithm can register the "wrong" repeated sequence.  Retry...
        mods = wiredtiger.wiredtiger_calc_modify(None, oldv, newv, nmod * (64 + repeatsz), nmod)

    return (oldv, mods, newv)
