/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * This file sets up our skip lists. In absence of C++ templates, we generate our own skip list
 * variants for different data types.
 *
 * This file demonstrates how we can do this using C preprocessor: Set up different macros for
 * different types, function names, and pieces of functionality, and then include the template
 * header file, which will instantiate them. We can then repeat this for other types.
 *
 * An alternative approach would be to use a Python generator, which would take a template file as
 * an input, and write out a file such as skiplist_auto.h with all the relevant type and function
 * names already filled in. There are some tradeoffs that we can explore, such as the ease of
 * development without the need for rerunning the generator (and needing a well-configured VS Code
 * for code navigation and completion to work) vs. code readability when not using an IDE (and thus
 * wanting a .h file with the names populated in).
 *
 * The rest of this PoC would be almost the same if we switch to using a Python generator.
 */

/*
 * NOTE: We would like the skip list template to define its own types for the cursor and for the
 * skip list head, but that would take a lot more refactoring than its worth doing for the first
 * PoC. We will do it if we all agree that this is the right approach for our skip list refactoring.
 *
 * Most importantly, we would need to refactor several fields from WT_CURSOR_BTREE into its own
 * struct and also rename "ins" to something else. This is not particularly challenging, but it's
 * not worth doing at this point, as it would result in a huge PR. This way, the PR would be kept at
 * a much more reasonable size.
 */

/*
 * Skip list macros that work irrespective of the type.
 */
#define WT_SKIP_FIRST(ins_head) (((ins_head) == NULL) ? NULL : (ins_head)->head[0])
#define WT_SKIP_LAST(ins_head) (((ins_head) == NULL) ? NULL : (ins_head)->tail[0])
#define WT_SKIP_NEXT(ins) ((ins)->next[0])
#define WT_SKIP_FOREACH(ins, ins_head) \
    for ((ins) = WT_SKIP_FIRST(ins_head); (ins) != NULL; (ins) = WT_SKIP_NEXT(ins))

/*
 * Skip list definitions for WT_INSERT for the row store.
 */

/* Type names. */
#define TMPL_CURSOR WT_CURSOR_BTREE
#define TMPL_ELEMENT WT_INSERT
#define TMPL_HEAD WT_INSERT_HEAD
#define TMPL_KEY WT_ITEM

/* Key functionality. */
#define TMPL_KEY_ASSIGN(key, element)              \
    {                                              \
        (key)->data = WT_INSERT_KEY(element);      \
        (key)->size = WT_INSERT_KEY_SIZE(element); \
    }
#define TMPL_KEY_COMPARE(session, srch_key, key, cmp) \
    WT_RET(__wt_compare(session, S2BT(session)->collator, srch_key, key, cmp));
#define TMPL_KEY_COMPARE_SKIP(session, srch_key, key, cmp, match) \
    WT_RET(__wt_compare_skip(session, S2BT(session)->collator, srch_key, key, cmp, match));

/* Functions to define. */
#define TMPL_FN_APPEND_SEARCH __wt_skip_append_search__insert
#define TMPL_FN_INSERT_SEARCH __wt_skip_insert_search__insert
#define TMPL_FN_INSERT_INTERNAL __wt_skip_insert_internal__insert
#define TMPL_FN_INSERT __wt_skip_insert__insert
#define TMPL_FN_CONTAINS __wt_skip_contains__insert

/* Let the preprocessor do its magic. */
#include "skiplist_template.h"

/*
 * Skip list definitions for an int (for testing).
 *
 * This will probably be eventually separated out into the test code, but for now, keeping it here
 * to demonstrate adding a skip list with another data type.
 *
 * If we like this, we would add here a skip list definition that is used in our column store. This
 * would also store WT_INSERT, but the key and the key comparison functions will be for recno. This
 * is not worth doing at this point, given the work involved in the relevant refactoring.
 */

/*
 * Types. Again, please note that we would like the template to generate them automatically, but
 * that would require more refactoring than is worth to do at this stage. For now, please pretend
 * that this was generated automatically.
 */
struct __wt_int_node {
    int key;

    struct __wt_int_node *next[0]; /* Forward-linked skip list. */
};

typedef struct __wt_int_node WT_INT_NODE;

struct __wt_int_head {
    WT_INT_NODE *head[WT_SKIP_MAXDEPTH]; /* first item on skiplists */
    WT_INT_NODE *tail[WT_SKIP_MAXDEPTH]; /* last item on skiplists */
};

typedef struct __wt_int_head WT_INT_HEAD;

struct __wt_int_cursor {
    WT_INT_HEAD *ins_head;                     /* Insert chain head */
    WT_INT_NODE *ins;                          /* Current node */
    WT_INT_NODE **ins_stack[WT_SKIP_MAXDEPTH]; /* Search stack */
    WT_INT_NODE *next_stack[WT_SKIP_MAXDEPTH]; /* Next item(s) found during search */
    int compare;                               /* Result of the last comparison */
};

typedef struct __wt_int_cursor WT_INT_CURSOR;

struct __wt_int_skiplist {
    WT_INT_HEAD head;
    WT_SPINLOCK lock; /* Needed only when updating the "tail" of the list. */
};

typedef struct __wt_int_skiplist WT_INT_SKIPLIST;

static inline int
__wt_int_compare(int a, int b)
{
    return (a == b ? 0 : (a < b ? -1 : 1));
}

static inline int
__wt_int_compare_p(const int *ap, const int *bp)
{
    return (__wt_int_compare(*ap, *bp));
}

/* Type names. */
#define TMPL_CURSOR WT_INT_CURSOR
#define TMPL_ELEMENT WT_INT_NODE
#define TMPL_HEAD WT_INT_HEAD
#define TMPL_KEY int

/* Key functionality. */
#define TMPL_KEY_ASSIGN(keyp, element) *(keyp) = element->key;
#define TMPL_KEY_COMPARE(session, srch_key, key, cmp) *(cmp) = __wt_int_compare_p(srch_key, key);
#define TMPL_KEY_COMPARE_SKIP(session, srch_key, key, cmp, match) \
    *(cmp) = __wt_int_compare_p(srch_key, key);

/* Functions to define. */
#define TMPL_FN_APPEND_SEARCH __wt_skip_append_search__int
#define TMPL_FN_INSERT_SEARCH __wt_skip_insert_search__int
#define TMPL_FN_INSERT_INTERNAL __wt_skip_insert_internal__int
#define TMPL_FN_INSERT __wt_skip_insert__int
#define TMPL_FN_CONTAINS __wt_skip_contains__int

/* Let the preprocessor do its magic. */
#include "skiplist_template.h"
