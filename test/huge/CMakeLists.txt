
create_test_executable(test_huge
    SOURCES huge.c
)

define_test_variants(test_huge
    VARIANTS
        "small|-s"
    LABELS
        check
)
