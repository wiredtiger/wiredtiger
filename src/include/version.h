/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_VERSION --
 *	Representation of WiredTiger version information.
 */
struct __wt_version {
    uint16_t major;
    uint16_t minor;
    uint16_t patch;
};

/*
 * WiredTiger version to use when none is present.
 */
#define WT_NO_VALUE UINT16_MAX
#define WT_NO_VERSION ((WT_VERSION){WT_NO_VALUE, WT_NO_VALUE, WT_NO_VALUE})

/*
 * __wt_version_cmp --
 *     Compare two version numbers and return if the first version number is greater than, equal to,
 *     or less than the second. Return the value as an int similar to strcmp().
 */
static inline int32_t
__wt_version_cmp(WT_VERSION version, WT_VERSION other)
{
    /*
     * The patch version is not always set for both inputs. In these cases we ignore comparison of
     * patch version by setting them both to the same value Structs are pass-by-value so this will
     * not modify the versions being compared.
     */
    if (version.patch == WT_NO_VALUE || other.patch == WT_NO_VALUE) {
        version.patch = other.patch = 0;
    }

    if (version.major == other.major && version.minor == other.minor &&
      version.patch == other.patch)
        return 0;

    if (version.major > other.major)
        return 1;
    if (version.major == other.major && version.minor > other.minor)
        return 1;
    if (version.major == other.major && version.minor == other.minor && version.patch > other.patch)
        return 1;

    return -1;
}

/*
 * __wt_version_eq --
 *     Return true if the two provided versions are equal.
 */
static inline bool
__wt_version_eq(WT_VERSION version, WT_VERSION other)
{
    return __wt_version_cmp(version, other) == 0;
}

/*
 * __wt_version_defined --
 *     Return true if the version has been properly defined with non-default values. Valid versions
 *     do not require the patch version to be set.
 */
static inline bool
__wt_version_defined(WT_VERSION version)
{
    return version.major != WT_NO_VALUE && version.minor != WT_NO_VALUE;
}

/*
 * __wt_version_lt --
 *     Return true if a provided version is less than the other version.
 */
static inline bool
__wt_version_lt(WT_VERSION version, WT_VERSION other)
{
    return __wt_version_cmp(version, other) == -1;
}

/*
 * __wt_version_lt --
 *     Return true if a provided version is less than or equal to the other version.
 */
static inline bool
__wt_version_lte(WT_VERSION version, WT_VERSION other)
{
    return __wt_version_cmp(version, other) != 1;
}

/*
 * __wt_version_gt --
 *     Return true if a provided version is greater than the other version.
 */
static inline bool
__wt_version_gt(WT_VERSION version, WT_VERSION other)
{
    return __wt_version_cmp(version, other) == 1;
}

/*
 * __wt_version_lt --
 *     Return true if a provided version is greater than or equal to the other version.
 */
static inline bool
__wt_version_gte(WT_VERSION version, WT_VERSION other)
{
    return __wt_version_cmp(version, other) != -1;
}
