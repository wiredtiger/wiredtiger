/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef	__WIREDTIGER_EXT_H_
#define	__WIREDTIGER_EXT_H_

#include <wiredtiger.h>

#if defined(__cplusplus)
extern "C" {
#endif

#if !defined(SWIG)

/*! @addtogroup wt_ext
 * @{
 */

/*! Table of WiredTiger extension methods.
 *
 * This structure is used to provide a set of WiredTiger methods to extension
 * modules without needing to link the modules with the WiredTiger library.
 *
 * The extension methods may be used both by modules that are linked with
 * the WiredTiger library (for example, a data source configured using the
 * WT_CONNECTION::add_data_source method), and by modules not linked with the
 * WiredTiger library (for example, a compression module configured using the
 * WT_CONNECTION::add_compressor method).
 *
 * To use these functions:
 * - include the wiredtiger_ext.h header file,
 * - declare a variable which references a WT_EXTENSION_API structure, and
 * - initialize the variable using WT_CONNECTION::get_extension_api method.
 *
 * @snippet ex_data_source.c WT_EXTENSION_API declaration
 *
 * The following code is from the sample compression module, where the
 * extension functions are configured  in the extension's entry point:
 *
 * @snippet nop_compress.c WT_EXTENSION_API declaration
 * @snippet nop_compress.c WT_EXTENSION_API initialization
 */
struct __wt_extension_api {
/* !!! To maintain backwards compatibility, this structure is append-only. */
#if !defined(DOXYGEN)
	/*
	 * Private fields.
	 */
	WT_CONNECTION *conn;		/* Enclosing connection */
#endif
	/*!
	 * Insert an error message into the WiredTiger error stream.
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param fmt a printf-like format specification
	 * @errors
	 *
	 * @snippet ex_data_source.c WT_EXTENSION_API err_printf
	 */
	int (*err_printf)(WT_EXTENSION_API *wt_api,
	    WT_SESSION *session, const char *fmt, ...);

	/*!
	 * Insert a message into the WiredTiger message stream.
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param fmt a printf-like format specification
	 * @errors
	 *
	 * @snippet ex_data_source.c WT_EXTENSION_API msg_printf
	 */
	int (*msg_printf)(
	    WT_EXTENSION_API *, WT_SESSION *session, const char *fmt, ...);

	/*!
	 * Allocate short-term use scratch memory.
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param bytes the number of bytes of memory needed
	 * @returns A valid memory reference on success or NULL on error
	 *
	 * @snippet ex_data_source.c WT_EXTENSION_API scr_alloc
	 */
	void *(*scr_alloc)(
	    WT_EXTENSION_API *wt_api, WT_SESSION *session, size_t bytes);

	/*!
	 * Free short-term use scratch memory.
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param ref a memory reference returned by WT_EXTENSION_API::scr_alloc
	 *
	 * @snippet ex_data_source.c WT_EXTENSION_API scr_free
	 */
	void (*scr_free)(WT_EXTENSION_API *, WT_SESSION *session, void *ref);

	/*!
	 * Return the value of a configuration string.
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param key configuration key string
	 * @param config the configuration information passed to an application
	 * callback
	 * @param value the returned value
	 * @errors
	 *
	 * @snippet ex_data_source.c WT_EXTENSION config_get
	 */
	int (*config_get)(WT_EXTENSION_API *wt_api, WT_SESSION *session,
	    WT_CONFIG_ARG *config, const char *key, WT_CONFIG_ITEM *value);

	/*!
	 * Return the list entries of a configuration string value.
	 * This method steps through the entries found in the last returned
	 * value from WT_EXTENSION_API::config_get.  The last returned value
	 * should be of type "list".
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param value the returned value
	 * @errors
	 *
	 * @snippet ex_data_source.c WT_EXTENSION config scan
	 */
	int (*config_scan_begin)(WT_EXTENSION_API *wt_api, WT_SESSION *session,
	    const char *str, size_t len, WT_CONFIG_SCAN **scanp);

	/*!
	 * Release any resources allocated by
	 * WT_EXTENSION_API::config_scan_begin.
	 *
	 * @param wt_api the extension handle
	 * @param scan the configuration scanner, invalid after this call
	 * @errors
	 *
	 * @snippet ex_data_source.c WT_EXTENSION config scan
	 */
	int (*config_scan_end)(WT_EXTENSION_API *wt_api, WT_CONFIG_SCAN *scan);

	/*!
	 * Return the next key/value pair from a config string scan.
	 *
	 * If the string contains a list of items with no assigned value, the
	 * items will be returned in \c key and the \c value will be set to the
	 * boolean \c "true" value.
	 *
	 * @param wt_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param str the configuration string to scan
	 * @param len the number of valid bytes in \c str
	 * @param[out] scanp a handle used to scan the config string
	 * @errors
	 *
	 * @snippet ex_data_source.c WT_EXTENSION config scan
	 */
	int (*config_scan_next)(WT_EXTENSION_API *wt_api,
	    WT_CONFIG_SCAN *scan, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value);
};

/*!
 * @typedef WT_CONFIG_ARG
 *
 * A configuration object passed to some extension interfaces.  This is an
 * opaque type: configuration values can be queried using
 * WT_EXTENSION_API::config_get.
 */

/*!
 * The configuration information returned by the WiredTiger extension function
 * WT_EXTENSION_API::config_get.
 */
struct __wt_config_item {
	/*!
	 * The value of a configuration string.
	 *
	 * Regardless of the type of the configuration string (boolean, int,
	 * list or string), the \c str field will reference the value of the
	 * configuration string.
	 *
	 * The bytes referenced by \c str are <b>not</b> be nul-terminated,
	 * use the \c len field instead of a terminating nul byte.
	 */
	const char *str;

	/*! The number of bytes in the value referenced by \c str. */
	size_t len;

	/*!
	 * The value of a configuration boolean or integer.
	 *
	 * If the configuration string's value is "true" or "false", the
	 * \c val field will be set to 1 (true), or 0 (false).
	 *
	 * If the configuration string can be legally interpreted as an integer,
	 * using the strtoll function rules as specified in ISO/IEC 9899:1990
	 * ("ISO C90"), that integer will be stored in the \c val field.
	 */
	int64_t val;

	/*! Permitted values of the \c type field. */
	enum {
		/*! A string value with quotes stripped. */
		WT_CONFIG_ITEM_STRING,
		/*! A boolean literal ("true" or "false"). */
		WT_CONFIG_ITEM_BOOL,
		/*! An unquoted identifier: a string value without quotes. */
		WT_CONFIG_ITEM_ID,
		/*! A numeric value. */
		WT_CONFIG_ITEM_NUM,
		/*! A nested structure or list, including brackets. */
		WT_CONFIG_ITEM_STRUCT
	}
	/*!
	 * The type of value determined by the parser.  In all cases,
	 * the \c str and \c len fields are set.
	 */
	type;
};

/*!
 * @typedef WT_CONFIG_SCAN
 *
 * A handle for a scan through a configuration string.
 * This is an opaque type returned by WT_EXTENSION_API::config_scan_begin.
 * Configuration values can be queried using WT_EXTENSION_API::config_scan_next.
 * Call WT_EXTENSION_API::config_scan_end when finished to release resources.
 */

/*! @} */
#endif /* SWIG */

#if defined(__cplusplus)
}
#endif
#endif /* __WIREDTIGER_EXT_H_ */
