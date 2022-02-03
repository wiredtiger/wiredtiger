#include <memory>

#include <catch2/catch.hpp>

#include "wt_internal.h"

WT_EXT* create_new_ext() {
    // manually alloc enough extra space for the zero-length array to encode two
    // skip lists.
    auto sz = sizeof(WT_EXT) + 2 * WT_SKIP_MAXDEPTH * sizeof(WT_EXT*);

    auto ret = (WT_EXT*)malloc(sz);
    memset(ret, 0, sz);

    return ret;
}

std::unique_ptr<WT_SIZE> create_new_sz() {
    auto ret = std::unique_ptr<WT_SIZE>((WT_SIZE*)malloc(sizeof(WT_SIZE)));
    memset(ret.get(), 0, sizeof(WT_SIZE));

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

/*
 * Creates a sane-looking "default" extent list suitable for testing:
 * L0: 1 -> 2 -> 3 -> X
 * L1: 2 -> 3 -> X
 * L2: 3 -> X
 * L3: X
 * ...
 * L9: X
 */
void create_default_test_extent_list(std::vector<WT_EXT*>& head) {
    auto first = create_new_ext();
    auto second = create_new_ext();
    auto third = create_new_ext();
    first->next[0] = second;
    first->next[1] = third;
    second->next[0] = third;

    head[0] = first;
    head[1] = second;
    head[2] = third;
    for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
        head[i] = nullptr;
}

void create_default_test_size_list(std::vector<WT_SIZE*>& head) {
    auto first = create_new_sz();
    auto second = create_new_sz();
    auto third = create_new_sz();
    first->next[0] = second.get();
    first->next[1] = third.get();
    second->next[0] = third.get();

    head[0] = first.get();
    head[1] = second.get();
    head[2] = third.get();
    for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
        head[i] = nullptr;
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

    SECTION("list with one element has non-empty final element") {
        auto first = create_new_ext();
        head[0] = first;

        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == head[0]);
    }

    SECTION("list with identical skip entries returns identical stack entries") {
        auto first = create_new_ext();
        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = first;

        __ut_block_off_srch_last(&head[0], &stack[0]);

        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++) {
            REQUIRE(stack[i] == &head[i]->next[i]);
        }
    }

    SECTION("list with differing skip entries returns differing stack entries") {
        create_default_test_extent_list(head);

        __ut_block_off_srch_last(&head[0], &stack[0]);

        REQUIRE(stack[0] == &head[2]->next[0]);
        REQUIRE(stack[1] == &head[2]->next[1]);
        REQUIRE(stack[2] == &head[2]->next[2]);
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++) {
            REQUIRE(stack[i] == &head[i]);
        }
    }

    SECTION("list with differing skip entries returns final entry") {
        auto first = create_new_ext();
        auto second = create_new_ext();
        first->next[0] = second;

        head[0] = first;
        for (int i = 1; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = second;

        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == second);
    }
}

TEST_CASE("block_off_srch", "[extent_list]") {
    std::vector<WT_EXT*> head(WT_SKIP_MAXDEPTH, nullptr);
    std::vector<WT_EXT**> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("can't find offset in empty list") {
        __ut_block_off_srch(&head[0], 0, &stack[0], false);

        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++)
            REQUIRE(stack[i] == &head[i]);
    }

    SECTION("exact offset match returns matching list element") {
        create_default_test_extent_list(head);

        head[0]->off = 1;
        head[1]->off = 2;
        head[2]->off = 3;

        __ut_block_off_srch(&head[0], 2, &stack[0], false);

        // for each level of the extent list, if the searched-for element was
        // visible, we should point to it. otherwise, we should point to the
        // next-largest item.
        REQUIRE((*stack[0])->off == 2);
        REQUIRE((*stack[1])->off == 2);
        REQUIRE((*stack[2])->off == 3);
    }

    SECTION("search for item larger than maximum in list returns end of list") {
        create_default_test_extent_list(head);

        head[0]->off = 1;
        head[1]->off = 2;
        head[2]->off = 3;

        __ut_block_off_srch(&head[0], 4, &stack[0], false);

        REQUIRE(stack[0] == &head[2]->next[0]);
        REQUIRE(stack[1] == &head[2]->next[1]);
        REQUIRE(stack[2] == &head[2]->next[2]);
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            REQUIRE(stack[i] == &head[i]);
    }

    SECTION("respect skip offset") {
        const int depth = 10;

        create_default_test_extent_list(head);

        head[0]->next[0] = nullptr;
        head[1]->next[1] = nullptr;
        head[2]->next[0] = nullptr;

        head[0]->next[0 + depth] = head[1];
        head[1]->next[1 + depth] = head[2];
        head[2]->next[0 + depth] = head[2];

        head[0]->off = 1;
        head[0]->depth = depth;
        head[1]->off = 2;
        head[1]->depth = depth;
        head[2]->off = 3;
        head[2]->depth = depth;

        __ut_block_off_srch(&head[0], 2, &stack[0], true);

        // for each level of the extent list, if the searched-for element was
        // visible, we should point to it. otherwise, we should point to the
        // next-largest item.
        REQUIRE((*stack[0])->off == 2);
        REQUIRE((*stack[1])->off == 2);
        REQUIRE((*stack[2])->off == 3);
    }
}

TEST_CASE("block_first_srch", "[extent_list]") {
    std::vector<WT_EXT*> head(WT_SKIP_MAXDEPTH, nullptr);
    std::vector<WT_EXT**> stack(WT_SKIP_MAXDEPTH, nullptr);

    // Note that we're not checking stack here, since __block_first_srch
    // delegates most of its work to __block_off_srch, which we're testing
    // elsewhere.

    SECTION("empty list doesn't yield a chunk") {
        REQUIRE(__ut_block_first_srch(&head[0], 0, &stack[0]) == false);
    }

    SECTION("list with too-small chunks doesn't yield a larger chunk") {
        create_default_test_extent_list(head);

        head[0]->size = 1;
        head[1]->size = 2;
        head[2]->size = 3;

        REQUIRE(__ut_block_first_srch(&head[0], 4, &stack[0]) == false);
    }

    SECTION("find an appropriate chunk") {
        create_default_test_extent_list(head);

        head[0]->size = 10;
        head[1]->size = 20;
        head[2]->size = 30;

        REQUIRE(__ut_block_first_srch(&head[0], 4, &stack[0]) == true);
    }
}

TEST_CASE("block_size_srch", "[extent_list]") {
    std::vector<WT_SIZE*> head(WT_SKIP_MAXDEPTH, nullptr);
    std::vector<WT_SIZE**> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("empty size list yields first elements") {
        __ut_block_size_srch(&head[0], 0, &stack[0]);

        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++)
            REQUIRE(stack[i] == &head[i]);
    }

    SECTION("exact size match returns matching list element") {
        create_default_test_size_list(head);

        head[0]->size = 1;
        head[1]->size = 2;
        head[2]->size = 3;

        __ut_block_size_srch(&head[0], 2, &stack[0]);

        // for each level of the extent list, if the searched-for element was
        // visible, we should point to it. otherwise, we should point to the
        // next-largest item.
        REQUIRE((*stack[0])->size == 2);
        REQUIRE((*stack[1])->size == 2);
        REQUIRE((*stack[2])->size == 3);
    }

    SECTION("search for item larger than maximum in list returns end of list") {
        create_default_test_size_list(head);

        head[0]->size = 1;
        head[1]->size = 2;
        head[2]->size = 3;

        __ut_block_size_srch(&head[0], 4, &stack[0]);

        REQUIRE(stack[0] == &head[2]->next[0]);
        REQUIRE(stack[1] == &head[2]->next[1]);
        REQUIRE(stack[2] == &head[2]->next[2]);
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            REQUIRE(stack[i] == &head[i]);
    }
}
