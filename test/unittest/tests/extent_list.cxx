#include <memory>

#include <catch2/catch.hpp>

#include "wt_internal.h"

std::unique_ptr<WT_EXT> create_new_extent_list() {
    // manually alloc enough extra space for the zero-length array to encode two
    // skip lists.
    auto sz = sizeof(WT_EXT) + 2 * WT_SKIP_MAXDEPTH * sizeof(WT_EXT*);

    auto ret = std::unique_ptr<WT_EXT>((WT_EXT*)malloc(sz));
    memset(ret.get(), 0, sz);

    return ret;
}

std::unique_ptr<WT_SIZE> create_new_size_list() {
    auto ret = std::unique_ptr<WT_SIZE>(new WT_SIZE{0 ,0, {}, {}});
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
        auto first = create_new_extent_list();
        head[0] = first.get();

        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == head[0]);
    }

    SECTION("list with identical skip entries returns identical stack entries") {
        auto first = create_new_extent_list();
        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = first.get();

        __ut_block_off_srch_last(&head[0], &stack[0]);

        for (int i = 0; i < WT_SKIP_MAXDEPTH; i++) {
            REQUIRE(stack[i] == &head[i]->next[i]);
        }
    }

    SECTION("list with differing skip entries returns differing stack entries") {
        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        auto third = create_new_extent_list();
        first->next[0] = second.get();
        first->next[1] = third.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
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

    SECTION("list with differing skip entries returns final entry") {
        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        first->next[0] = second.get();

        head[0] = first.get();
        for (int i = 1; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = second.get();

        REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == second.get());
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
        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        auto third = create_new_extent_list();
        first->next[0] = second.get();
        first->next[1] = third.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->off = 1;
        second->off = 2;
        third->off = 3;

        __ut_block_off_srch(&head[0], 2, &stack[0], false);

        // for each level of the extent list, if the searched-for element was
        // visible, we should point to it. otherwise, we should point to the
        // next-largest item.
        REQUIRE((*stack[0])->off == 2);
        REQUIRE((*stack[1])->off == 2);
        REQUIRE((*stack[2])->off == 3);
    }

    SECTION("search for item larger than maximum in list returns end of list") {
        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        auto third = create_new_extent_list();
        first->next[0] = second.get();
        first->next[1] = third.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->off = 1;
        second->off = 2;
        third->off = 3;

        __ut_block_off_srch(&head[0], 4, &stack[0], false);

        REQUIRE(stack[0] == &head[2]->next[0]);
        REQUIRE(stack[1] == &head[2]->next[1]);
        REQUIRE(stack[2] == &head[2]->next[2]);
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            REQUIRE(stack[i] == &head[i]);
    }

    SECTION("respect skip offset") {
        const int depth = 10;

        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        auto third = create_new_extent_list();

        first->next[0 + depth] = second.get();
        first->next[1 + depth] = third.get();
        second->next[0 + depth] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->off = 1;
        first->depth = depth;
        second->off = 2;
        second->depth = depth;
        third->off = 3;
        third->depth = depth;

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
        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        auto third = create_new_extent_list();
        first->next[0] = second.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->size = 1;
        second->size = 2;
        third->size = 3;

        REQUIRE(__ut_block_first_srch(&head[0], 4, &stack[0]) == false);
    }

    SECTION("find an appropriate chunk") {
        auto first = create_new_extent_list();
        auto second = create_new_extent_list();
        auto third = create_new_extent_list();
        first->next[0] = second.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->size = 10;
        second->size = 20;
        third->size = 30;

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
        auto first = create_new_size_list();
        auto second = create_new_size_list();
        auto third = create_new_size_list();
        first->next[0] = second.get();
        first->next[1] = third.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->size = 1;
        second->size = 2;
        third->size = 3;

        __ut_block_size_srch(&head[0], 2, &stack[0]);

        // for each level of the extent list, if the searched-for element was
        // visible, we should point to it. otherwise, we should point to the
        // next-largest item.
        REQUIRE((*stack[0])->size == 2);
        REQUIRE((*stack[1])->size == 2);
        REQUIRE((*stack[2])->size == 3);
    }

    SECTION("search for item larger than maximum in list returns end of list") {
        auto first = create_new_size_list();
        auto second = create_new_size_list();
        auto third = create_new_size_list();
        first->next[0] = second.get();
        first->next[1] = third.get();
        second->next[0] = third.get();

        head[0] = first.get();
        head[1] = second.get();
        head[2] = third.get();
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            head[i] = nullptr;

        first->size = 1;
        second->size = 2;
        third->size = 3;

        __ut_block_size_srch(&head[0], 4, &stack[0]);

        REQUIRE(stack[0] == &head[2]->next[0]);
        REQUIRE(stack[1] == &head[2]->next[1]);
        REQUIRE(stack[2] == &head[2]->next[2]);
        for (int i = 3; i < WT_SKIP_MAXDEPTH; i++)
            REQUIRE(stack[i] == &head[i]);
    }
}
