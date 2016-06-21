/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <ctype.h>

/*
 * __wt_isalnum --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isalnum(int c)
{
	return isalnum((unsigned char)c);
}

/*
 * __wt_isalpha --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isalpha(int c)
{
	return isalpha((unsigned char)c);
}

/*
 * __wt_iscntrl --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_iscntrl(int c)
{
	return iscntrl((unsigned char)c);
}

/*
 * __wt_isdigit --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isdigit(int c)
{
	return isdigit((unsigned char)c);
}

/*
 * __wt_isgraph --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isgraph(int c)
{
	return isgraph((unsigned char)c);
}

/*
 * __wt_islower --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_islower(int c)
{
	return islower((unsigned char)c);
}

/*
 * __wt_isprint --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isprint(int c)
{
	return isprint((unsigned char)c);
}

/*
 * __wt_ispunct --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_ispunct(int c)
{
	return ispunct((unsigned char)c);
}

/*
 * __wt_isspace --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isspace(int c)
{
	return isspace((unsigned char)c);
}

/*
 * __wt_isupper --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isupper(int c)
{
	return isupper((unsigned char)c);
}

/*
 * __wt_isxdigit --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_isxdigit(int c)
{
	return isxdigit((unsigned char)c);
}

/*
 * __wt_tolower --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_tolower(int c)
{
	return tolower((unsigned char)c);
}

/*
 * __wt_toupper --
 *	Wrap the ctype function without sign extension.
 */
static inline int
__wt_toupper(int c)
{
	return toupper((unsigned char)c);
}
