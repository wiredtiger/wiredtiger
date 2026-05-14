#pragma once

#include <fff.h>

#include "wiredtiger.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, ingest_search, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, stable_search, WT_CURSOR *);

DECLARE_FAKE_VALUE_FUNC(int, ingest_insert, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, stable_insert, WT_CURSOR *);

DECLARE_FAKE_VALUE_FUNC(int, ingest_update, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, stable_update, WT_CURSOR *);

DECLARE_FAKE_VALUE_FUNC(int, ingest_remove, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, stable_remove, WT_CURSOR *);

DECLARE_FAKE_VALUE_FUNC(int, ingest_reset, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, stable_reset, WT_CURSOR *);

DECLARE_FAKE_VALUE_FUNC_VARARG(int, ingest_get_key, WT_CURSOR *, ...);
DECLARE_FAKE_VALUE_FUNC_VARARG(int, stable_get_key, WT_CURSOR *, ...);

DECLARE_FAKE_VALUE_FUNC(WT_ITEM, ingest_get_key_item, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(WT_ITEM, stable_get_key_item, WT_CURSOR *);

DECLARE_FAKE_VALUE_FUNC_VARARG(int, ingest_get_value, WT_CURSOR *, ...);
DECLARE_FAKE_VALUE_FUNC_VARARG(int, stable_get_value, WT_CURSOR *, ...);

DECLARE_FAKE_VALUE_FUNC(WT_ITEM, ingest_get_value_item, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(WT_ITEM, stable_get_value_item, WT_CURSOR *);

DECLARE_FAKE_VOID_FUNC_VARARG(ingest_set_key, WT_CURSOR *, ...);
DECLARE_FAKE_VOID_FUNC_VARARG(stable_set_key, WT_CURSOR *, ...);

DECLARE_FAKE_VOID_FUNC(ingest_set_key_item, WT_CURSOR *, WT_ITEM);
DECLARE_FAKE_VOID_FUNC(stable_set_key_item, WT_CURSOR *, WT_ITEM);

DECLARE_FAKE_VOID_FUNC_VARARG(ingest_set_value, WT_CURSOR *, ...);
DECLARE_FAKE_VOID_FUNC_VARARG(stable_set_value, WT_CURSOR *, ...);

DECLARE_FAKE_VOID_FUNC(ingest_set_value_item, WT_CURSOR *, WT_ITEM);
DECLARE_FAKE_VOID_FUNC(stable_set_value_item, WT_CURSOR *, WT_ITEM);
}

void reset_cur_layered_fakes();
