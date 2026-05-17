#include "fake_cur_layered.h"

#include <cstdarg>

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, ingest_search, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, stable_search, WT_CURSOR *);

DEFINE_FAKE_VALUE_FUNC(int, ingest_insert, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, stable_insert, WT_CURSOR *);

DEFINE_FAKE_VALUE_FUNC(int, ingest_update, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, stable_update, WT_CURSOR *);

DEFINE_FAKE_VALUE_FUNC(int, ingest_remove, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, stable_remove, WT_CURSOR *);

DEFINE_FAKE_VALUE_FUNC(int, ingest_reset, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(int, stable_reset, WT_CURSOR *);

DEFINE_FAKE_VALUE_FUNC_VARARG(int, ingest_get_key, WT_CURSOR *, ...);
DEFINE_FAKE_VALUE_FUNC_VARARG(int, stable_get_key, WT_CURSOR *, ...);

DEFINE_FAKE_VALUE_FUNC(WT_ITEM, ingest_get_key_item, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(WT_ITEM, stable_get_key_item, WT_CURSOR *);

DEFINE_FAKE_VALUE_FUNC_VARARG(int, ingest_get_value, WT_CURSOR *, ...);
DEFINE_FAKE_VALUE_FUNC_VARARG(int, stable_get_value, WT_CURSOR *, ...);

DEFINE_FAKE_VALUE_FUNC(WT_ITEM, ingest_get_value_item, WT_CURSOR *);
DEFINE_FAKE_VALUE_FUNC(WT_ITEM, stable_get_value_item, WT_CURSOR *);

DEFINE_FAKE_VOID_FUNC_VARARG(ingest_set_key, WT_CURSOR *, ...);
DEFINE_FAKE_VOID_FUNC_VARARG(stable_set_key, WT_CURSOR *, ...);

DEFINE_FAKE_VOID_FUNC(ingest_set_key_item, WT_CURSOR *, WT_ITEM);
DEFINE_FAKE_VOID_FUNC(stable_set_key_item, WT_CURSOR *, WT_ITEM);

DEFINE_FAKE_VOID_FUNC_VARARG(ingest_set_value, WT_CURSOR *, ...);
DEFINE_FAKE_VOID_FUNC_VARARG(stable_set_value, WT_CURSOR *, ...);

DEFINE_FAKE_VOID_FUNC(ingest_set_value_item, WT_CURSOR *, WT_ITEM);
DEFINE_FAKE_VOID_FUNC(stable_set_value_item, WT_CURSOR *, WT_ITEM);

DEFINE_FAKE_VALUE_FUNC(int, __clayered_reserve_constituent, WT_SESSION_IMPL *, WT_CURSOR *);
}

namespace {

// Hooks to pack/unpack between va_list and WT_ITEM:

int
ingest_get_key_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, WT_ITEM *);

    if (item != nullptr)
        *item = ingest_get_key_item(c);

    return ingest_get_key_fake.return_val;
}

int
stable_get_key_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, WT_ITEM *);

    if (item != nullptr)
        *item = stable_get_key_item(c);

    return stable_get_key_fake.return_val;
}

int
ingest_get_value_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, WT_ITEM *);

    if (item != nullptr)
        *item = ingest_get_value_item(c);

    return ingest_get_value_fake.return_val;
}

int
stable_get_value_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, WT_ITEM *);

    if (item != nullptr)
        *item = stable_get_value_item(c);

    return stable_get_value_fake.return_val;
}

void
ingest_set_key_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, const WT_ITEM *);
    ingest_set_key_item(c, *item);
}

void
stable_set_key_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, const WT_ITEM *);
    stable_set_key_item(c, *item);
}

void
ingest_set_value_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, const WT_ITEM *);
    ingest_set_value_item(c, *item);
}

void
stable_set_value_va(WT_CURSOR *c, va_list ap)
{
    const auto item = va_arg(ap, const WT_ITEM *);
    stable_set_value_item(c, *item);
}

} // namespace

void
reset_cur_layered_fakes()
{
    RESET_FAKE(ingest_search);
    RESET_FAKE(stable_search);
    RESET_FAKE(ingest_insert);
    RESET_FAKE(stable_insert);
    RESET_FAKE(ingest_update);
    RESET_FAKE(stable_update);
    RESET_FAKE(ingest_remove);
    RESET_FAKE(stable_remove);
    RESET_FAKE(ingest_reset);
    RESET_FAKE(stable_reset);
    RESET_FAKE(ingest_get_key);
    RESET_FAKE(stable_get_key);
    RESET_FAKE(ingest_get_key_item);
    RESET_FAKE(stable_get_key_item);
    RESET_FAKE(ingest_get_value);
    RESET_FAKE(stable_get_value);
    RESET_FAKE(ingest_get_value_item);
    RESET_FAKE(stable_get_value_item);
    RESET_FAKE(ingest_set_key);
    RESET_FAKE(stable_set_key);
    RESET_FAKE(ingest_set_key_item);
    RESET_FAKE(stable_set_key_item);
    RESET_FAKE(ingest_set_value);
    RESET_FAKE(stable_set_value);
    RESET_FAKE(ingest_set_value_item);
    RESET_FAKE(stable_set_value_item);
    RESET_FAKE(__clayered_reserve_constituent);

    ingest_get_key_fake.custom_fake = ingest_get_key_va;
    stable_get_key_fake.custom_fake = stable_get_key_va;
    ingest_get_value_fake.custom_fake = ingest_get_value_va;
    stable_get_value_fake.custom_fake = stable_get_value_va;
    ingest_set_key_fake.custom_fake = ingest_set_key_va;
    stable_set_key_fake.custom_fake = stable_set_key_va;
    ingest_set_value_fake.custom_fake = ingest_set_value_va;
    stable_set_value_fake.custom_fake = stable_set_value_va;

    FFF_RESET_HISTORY();
}
