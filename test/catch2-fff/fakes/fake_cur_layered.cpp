#include "fake_cur_layered.h"

#include <cstdarg>

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, cursor_search, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, cursor_insert, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, cursor_update, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, cursor_remove, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, cursor_reset, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC_VARARG(int, cursor_get_key, WT_CURSOR *, ...);
DEFINE_FAKE_VALUE_FUNC(WT_ITEM, cursor_get_key_item, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC_VARARG(int, cursor_get_value, WT_CURSOR *, ...);
DEFINE_FAKE_VALUE_FUNC(WT_ITEM, cursor_get_value_item, WT_CURSOR *);
DEFINE_FAKE_VOID_FUNC_VARARG(cursor_set_key, WT_CURSOR *, ...);
DEFINE_FAKE_VOID_FUNC(cursor_set_key_item, WT_CURSOR *, WT_ITEM);
DEFINE_FAKE_VOID_FUNC_VARARG(cursor_set_value, WT_CURSOR *, ...);
DEFINE_FAKE_VOID_FUNC(cursor_set_value_item, WT_CURSOR *, WT_ITEM);
DEFINE_FAKE_VALUE_FUNC(int, __clayered_reserve_constituent, WT_SESSION_IMPL *, WT_CURSOR *);
}

namespace {

// Hooks to pack/unpack between va_list and WT_ITEM:

int
cursor_get_key_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, WT_ITEM *);

    if (item != nullptr)
        *item = cursor_get_key_item(c);

    return cursor_get_key_fake.return_val;
}

int
cursor_get_value_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, WT_ITEM *);

    if (item != nullptr)
        *item = cursor_get_value_item(c);

    return cursor_get_value_fake.return_val;
}

void
cursor_set_key_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, const WT_ITEM *);
    cursor_set_key_item(c, *item);
}

void
cursor_set_value_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, const WT_ITEM *);
    cursor_set_value_item(c, *item);
}

} // namespace

void
reset_cur_layered_fakes()
{
    RESET_FAKE(cursor_search);
    RESET_FAKE(cursor_insert);
    RESET_FAKE(cursor_update);
    RESET_FAKE(cursor_remove);
    RESET_FAKE(cursor_reset);
    RESET_FAKE(cursor_get_key);
    RESET_FAKE(cursor_get_key_item);
    RESET_FAKE(cursor_get_value);
    RESET_FAKE(cursor_get_value_item);
    RESET_FAKE(cursor_set_key);
    RESET_FAKE(cursor_set_key_item);
    RESET_FAKE(cursor_set_value);
    RESET_FAKE(cursor_set_value_item);
    RESET_FAKE(__clayered_reserve_constituent);

    cursor_get_key_fake.custom_fake = cursor_get_key_va;
    cursor_get_value_fake.custom_fake = cursor_get_value_va;
    cursor_set_key_fake.custom_fake = cursor_set_key_va;
    cursor_set_value_fake.custom_fake = cursor_set_value_va;

    FFF_RESET_HISTORY();
}
