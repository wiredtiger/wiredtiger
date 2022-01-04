project(test_huge C)

create_test_executable(test_huge
    SOURCES huge.c
)

define_test_variants(test_huge
    VARIANTS
        "test_huge_small;-s"
    LABELS
        check
)
