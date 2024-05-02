
#include "wiredtiger.h"
#include <string.h>
#include <catch2/catch.hpp>

#pragma once

void init_wt_item(WT_ITEM &item);
bool require_get_key_value(WT_CURSOR *cursor, const char *expected_key, const char *expected_value);
bool require_get_raw_key_value(
  WT_CURSOR *cursor, const char *expected_key, const char *expected_value);
bool check_item(WT_ITEM *item, const char *expected);
