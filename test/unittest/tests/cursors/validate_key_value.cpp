#include "validate_key_value.h"

bool
require_get_key_value(WT_CURSOR *cursor, const char *expected_key, const char *expected_value)
{
    const char *key = nullptr;
    const char *value = nullptr;
    REQUIRE(cursor->get_key(cursor, &key) == 0);
    REQUIRE(cursor->get_value(cursor, &value) == 0);

    bool keys_match = strcmp(key, expected_key) == 0;
    bool values_match = strcmp(value, expected_value) == 0;
    REQUIRE(keys_match);
    REQUIRE(values_match);

    return keys_match && values_match;
}

bool
require_get_raw_key_value(WT_CURSOR *cursor, const char *expected_key, const char *expected_value)
{
    WT_ITEM item_key;
    init_wt_item(item_key);
    WT_ITEM item_value;
    init_wt_item(item_value);

    WT_ITEM *p_item_key = (expected_key == nullptr) ? nullptr : &item_key;
    WT_ITEM *p_item_value = (expected_value == nullptr) ? nullptr : &item_value;

    REQUIRE(cursor->get_raw_key_value(cursor, p_item_key, p_item_value) == 0);

    bool keys_match = check_item(p_item_key, expected_key);
    bool values_match = check_item(p_item_value, expected_value);

    return keys_match && values_match;
}

void
init_wt_item(WT_ITEM &item)
{
    item.data = nullptr;
    item.size = 0;
    item.mem = nullptr;
    item.memsize = 0;
    item.flags = 0;
}

bool
check_item(WT_ITEM *item, const char *expected)
{
    bool match = true;
    if (expected != nullptr) {
        const char *key = static_cast<const char *>(item->data);
        REQUIRE(key != nullptr);
        match = strcmp(key, expected) == 0;
    }
    REQUIRE(match);
    return match;
}
