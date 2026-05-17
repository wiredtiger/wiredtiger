#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, cursor_search, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, cursor_insert, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, cursor_update, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, cursor_remove, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC(int, cursor_reset, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC_VARARG(int, cursor_get_key, WT_CURSOR *, ...);
DECLARE_FAKE_VALUE_FUNC(WT_ITEM, cursor_get_key_item, WT_CURSOR *);
DECLARE_FAKE_VALUE_FUNC_VARARG(int, cursor_get_value, WT_CURSOR *, ...);
DECLARE_FAKE_VALUE_FUNC(WT_ITEM, cursor_get_value_item, WT_CURSOR *);
DECLARE_FAKE_VOID_FUNC_VARARG(cursor_set_key, WT_CURSOR *, ...);
DECLARE_FAKE_VOID_FUNC(cursor_set_key_item, WT_CURSOR *, WT_ITEM);
DECLARE_FAKE_VOID_FUNC_VARARG(cursor_set_value, WT_CURSOR *, ...);
DECLARE_FAKE_VOID_FUNC(cursor_set_value_item, WT_CURSOR *, WT_ITEM);
DECLARE_FAKE_VALUE_FUNC(int, __clayered_reserve_constituent, WT_SESSION_IMPL *, WT_CURSOR *);
}

void reset_cur_layered_fakes();
