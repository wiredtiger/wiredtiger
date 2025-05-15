project(test_cursor_order C)

create_test_executable(test_cursor_order
    SOURCES
        cursor_order_file.c
        cursor_order_ops.c
        cursor_order.c
)

define_test_variants(test_cursor_order
    VARIANTS
        "test_cursor_order_row;"
    LABELS
        # Run this during a "ctest check" smoke test.
        check
        test_cursor_order
)
