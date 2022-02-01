#include <memory>

#include <catch2/catch.hpp>

#include "wt_internal.h"

WT_EXT* create_new_extent_list() {
    // manually alloc enough extra space for the zero-length array at the end
    auto sz = sizeof(WT_EXT) + WT_SKIP_MAXDEPTH * sizeof(WT_EXT*);

    WT_EXT* ret = (WT_EXT*)malloc(sz);
    memset(ret, 0, sz);

    return ret;
}

void print_list(WT_EXT **head) {
    WT_EXT* extp;
    int i;

    if (head == nullptr)
        return;

    for (i = 0; i < WT_SKIP_MAXDEPTH; i++) {
        printf("L%d: ", i);

        extp = head[i];
        while (extp != nullptr) {
            printf("%p -> ", extp);
            extp = extp->next[i];
        }

        printf("X\n");
    }
}

TEST_CASE("block_off_srch_last", "[extent_list]") {
    std::vector<WT_EXT*> head(WT_SKIP_MAXDEPTH, nullptr);
    std::vector<WT_EXT**> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("empty list has empty final element") {
        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == nullptr);

        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++) {
            REQUIRE(stack[i] == &head[i]);
        }
    }

    SECTION("list with one element has non-empty final element", "[extent_list]") {
        WT_EXT* first = create_new_extent_list();
        head[0] = first;

        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == first);
    }

    SECTION("list with identical skip entries returns identical stack entries", "[extent_list]") {
        WT_EXT* first = create_new_extent_list();
        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = first;

        __ut_block_off_srch_last(&head[0], &stack[0]);

        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++) {
            REQUIRE(stack[i] == &head[i]->next[i]);
        }
    }

    SECTION("list with differing skip entries returns differing stack entries", "[extent_list]") {
        WT_EXT* first = create_new_extent_list();
        WT_EXT* second = create_new_extent_list();
        WT_EXT* third = create_new_extent_list();
        first->next[0] = second;
        first->next[1] = third;
        second->next[0] = third;

        head[0] = first;
        head[1] = second;
        head[2] = third;
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        __ut_block_off_srch_last(&head[0], &stack[0]);

        REQUIRE(stack[0] == &third->next[0]);
        REQUIRE(stack[1] == &third->next[1]);
        REQUIRE(stack[2] == &third->next[2]);
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++) {
            REQUIRE(stack[i] == &head[i]);
        }
    }

    SECTION("list with differing skip entries returns final entry", "[extent_list]") {
        WT_EXT* first = create_new_extent_list();
        WT_EXT* second = create_new_extent_list();
        first->next[0] = second;

        head[0] = first;
        for (int i = 1; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = second;

        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == second);
    }
}
