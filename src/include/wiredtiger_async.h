/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef	__WIREDTIGER_ASYNC_H_
#define	__WIREDTIGER_ASYNC_H_

#if defined(__cplusplus)
extern "C" {
#endif

#include <wiredtiger.h>

#if defined(DOXYGEN) || defined(SWIG)
#define	__F(func) func
#else
#define	__F(func) (*func)
#endif

/*!
 * @defgroup wt_async WiredTiger Asynchronous API
 * The functions, handles and methods that applications use to access and
 * manage data through WiredTiger's asynchronous interface.
 *
 * @{
 */
struct __wt_async_callback;
		typedef struct __wt_async_callback WT_ASYNC_CALLBACK;
struct __wt_async_connection;
		typedef struct __wt_async_connection WT_ASYNC_CONNECTION;
struct __wt_async_ds;		typedef struct __wt_async_ds WT_ASYNC_DS;
struct __wt_async_op;		typedef struct __wt_async_op WT_ASYNC_OP;

#if defined(SWIGJAVA)
typedef WT_ASYNC_CONNECTION			WT_ASYNC_CONNECTION_CLOSED;
#endif

/*! Asynchronous operation types. */
typedef enum {
	WT_AOP_GET,		/*!< Search and return key/value pair */
	WT_AOP_INSERT,		/*!< Insert if key is not in the data source */
	WT_AOP_PUT,		/*!< Set the value for a key (unconditional) */
	WT_AOP_REMOVE,		/*!< Remove a key from the data source */
	WT_AOP_SCAN,		/*!< Full scan from the beginning of the data */
	WT_AOP_SCAN_FROM,	/*!< Scan from the specified key */
	WT_AOP_SCAN_REV,	/*!< Reverse full scan */
	WT_AOP_SCAN_REV_FROM,	/*!< Reverse scan from the specified key */
	WT_AOP_UPDATE		/*!< Set the value of an existing key */
} WT_ASYNC_OPTYPE;

/*!
 * A WT_ASYNC_OP is a queued asynchronous operation.
 */
struct __wt_async_op {
	/*! Operation type: maintained by WiredTiger. */
	WT_ASYNC_OPTYPE op;

	/*! Record number: maintained by WiredTiger. */
	WT_ASYNC_DS *ds;

	/*! Record number: maintained by WiredTiger. */
	uint64_t recno;

	/*! Key: maintained by WiredTiger. */
	WT_ITEM value;

	/*! Value: maintained by WiredTiger. */
	WT_ITEM key;

	/*! Application-owned data associated with this operation. */
	void *cookie;

#define	WT_ASYNC_OP_KEY_SET	0x01
#define	WT_ASYNC_OP_VALUE_SET	0x02
	/*! Flags: maintained by WiredTiger */
	uint32_t flags;
};

/*!
 * A WT_ASYNC_DS is the async interface to a data source in WiredTiger.
 */
struct __wt_async_ds {
	/*!
	 * The name of the data source for the cursor, matches the \c uri
	 * parameter to WT_SESSION::open_cursor used to open the cursor.
	 */
	const char *uri;

	/*!
	 * The format of the data packed into key items.  See @ref packing for
	 * details.  If not set, a default value of "u" is assumed, and
	 * applications must use WT_ITEM structures to manipulate untyped byte
	 * arrays.
	 */
	const char *key_format;

	/*!
	 * The format of the data packed into value items.  See @ref packing
	 * for details.  If not set, a default value of "u" is assumed, and
	 * applications must use WT_ITEM structures to manipulate untyped byte
	 * arrays.
	 */
	const char *value_format;

	/*!
	 * @name Data access
	 * @{
	 */
	/*!
	 * Get the key for an operation.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @param ... pointers to hold key fields corresponding to
	 * WT_CURSOR::key_format.
	 * @errors
	 */
	int __F(get_key)(WT_ASYNC_DS *ds, WT_ASYNC_OP *op, ...);

	/*!
	 * Get the value for an operation.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @param ... pointers to hold value fields corresponding to
	 * WT_CURSOR::value_format.
	 * @errors
	 */
	int __F(get_value)(WT_ASYNC_DS *ds, WT_ASYNC_OP *op, ...);

	/*!
	 * Set the key for an operation.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @param ... key fields corresponding to WT_CURSOR::key_format.
	 * @errors
	 */
	int __F(set_key)(WT_ASYNC_DS *ds, WT_ASYNC_OP *op, ...);

	/*!
	 * Set the value for the next operation.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @param ... value fields corresponding to WT_CURSOR::value_format.
	 * @errors
	 */
	int __F(set_value)(WT_ASYNC_DS *ds, WT_ASYNC_OP *op, ...);
	/*! @} */

	/*!
	 * @name Asynchronous operations
	 * @{
	 */
	/*! Allocate an operation structure.
	 *
	 * @param ds the async data source
	 * @param[out] opp a new async operation structure
	 * @errors
	 */
	int __F(alloc_op)(WT_ASYNC_DS *ds, WT_ASYNC_OP **opp);

	/*! Free an operation structure.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @errors
	 */
	int __F(free_op)(WT_ASYNC_DS *ds, WT_ASYNC_OP *op);

	/*! Execute an operation.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @param optype the async operation type
	 * @param cb A callback used to notify the application of the
	 * operation's progress (success, failure or records scanned).
	 * @errors
	 */
	int __F(execute)(WT_ASYNC_DS *ds,
	    WT_ASYNC_OP *op, WT_ASYNC_OPTYPE optype, WT_ASYNC_CALLBACK *cb);

	/*! Wait for an operation to complete.
	 *
	 * @param ds the async data source
	 * @param op the async operation
	 * @errors
	 */
	int __F(wait)(WT_ASYNC_DS *ds, WT_ASYNC_OP *op);
	/*! @} */
};

/*!
 * WT_ASYNC_CALLBACK is the interface applications implement to receive
 * notifications from asynchronous operations.
 */
struct __wt_async_callback {
	/*! An operation has completed.
	 *
	 * @param ret zero if the operation succeeded, non-zero otherwise.
	 */
	void (*complete)(WT_ASYNC_CALLBACK *cb, WT_ASYNC_OP *op, int ret);

	/*! Process an additional record in a scan.
	 *
	 * @returns ret zero if the operation succeeded, non-zero otherwise.
	 */
	int (*next)(WT_ASYNC_CALLBACK *cb, WT_ASYNC_OP *op);
};

/*!
 * A WT_ASYNC_CONNECTION is the async interface to a WiredTiger connection.
 */
struct __wt_async_connection {
	/*!
	 * Close a connection.
	 *
	 * Any open sessions will be closed.
	 *
	 * @snippet ex_all.c Close a connection
	 *
	 * @param connection the connection handle
	 * @configempty{async_conn.close, see dist/api_data.py}
	 * @configend
	 * @configempty{connection.close, see dist/api_data.py}
	 * @configend
	 * @errors
	 */
	int __F(close)(WT_HANDLE_CLOSED(WT_ASYNC_CONNECTION) *connection,
	    const char *config);

	/*!
	 * Get the synchronous connection, to perform some operation that
	 * doesn't have an async equivalent.
	 */
	int __F(get_conn)(
	    WT_ASYNC_CONNECTION *connection, WT_CONNECTION **connp);

	/*!
	 * Open a data source.
	 *
	 * See WT_SESSION::open_cursor for the synchronous version.
	 *
	 * @param connection the connection handle
	 * @param uri the data source.  See @ref data_sources for more
	 * information.
	 * @configempty{async_conn.open_data_source, see dist/api_data.py}
	 * @configstart{session.open_cursor, see dist/api_data.py}
	 * @config{append, append the value as a new record\, creating a new
	 * record number key; valid only for cursors with record number keys., a
	 * boolean flag; default \c false.}
	 * @config{bulk, configure the cursor for bulk-loading\, a fast\,
	 * initial load path (see @ref bulk_load for more information).
	 * Bulk-load may only be used for newly created objects and cursors
	 * configured for bulk-load only support the WT_CURSOR::insert and
	 * WT_CURSOR::close methods.  When bulk-loading row-store objects\, keys
	 * must be loaded in sorted order.  The value is usually a true/false
	 * flag; when bulk-loading fixed-length column store objects\, the
	 * special value \c bitmap allows chunks of a memory resident bitmap to
	 * be loaded directly into a file by passing a \c WT_ITEM to
	 * WT_CURSOR::set_value where the \c size field indicates the number of
	 * records in the bitmap (as specified by the object's \c value_format
	 * configuration). Bulk-loaded bitmap values must end on a byte boundary
	 * relative to the bit count (except for the last set of values
	 * loaded)., a string; default \c false.}
	 * @config{checkpoint, the name of a checkpoint to open (the reserved
	 * name "WiredTigerCheckpoint" opens the most recent internal checkpoint
	 * taken for the object). The cursor does not support data
	 * modification., a string; default empty.}
	 * @config{dump, configure the cursor for dump format inputs and
	 * outputs: "hex" selects a simple hexadecimal format\, "print" selects
	 * a format where only non-printing characters are hexadecimal encoded.
	 * The cursor dump format is compatible with the @ref util_dump and @ref
	 * util_load commands., a string\, chosen from the following options: \c
	 * "hex"\, \c "print"; default empty.}
	 * @config{next_random, configure the cursor to return a pseudo-random
	 * record from the object; valid only for row-store cursors.  Cursors
	 * configured with \c next_random=true only support the WT_CURSOR::next
	 * and WT_CURSOR::close methods.  See @ref cursor_random for details., a
	 * boolean flag; default \c false.}
	 * @config{overwrite, configures whether the cursor's insert\, update
	 * and remove methods check the existing state of the record.  If \c
	 * overwrite is \c false\, WT_CURSOR::insert fails with
	 * ::WT_DUPLICATE_KEY if the record exists\, WT_CURSOR::update and
	 * WT_CURSOR::remove fail with ::WT_NOTFOUND if the record does not
	 * exist., a boolean flag; default \c true.}
	 * @config{raw, ignore the encodings for the key and value\, manage data
	 * as if the formats were \c "u". See @ref cursor_raw for details., a
	 * boolean flag; default \c false.}
	 * @config{statistics, Specify the statistics to be gathered.  Choosing
	 * "all" gathers statistics regardless of cost and may include
	 * traversing on-disk files; "fast" gathers a subset of relatively
	 * inexpensive statistics.  The selection must agree with the database
	 * \c statistics configuration specified to ::wiredtiger_open or
	 * WT_CONNECTION::reconfigure.  For example\, "all" or "fast" can be
	 * configured when the database is configured with "all"\, but the
	 * cursor open will fail if "all" is specified when the database is
	 * configured with "fast"\, and the cursor open will fail in all cases
	 * when the database is configured with "none". If \c statistics is not
	 * configured\, the default configuration is the database configuration.
	 * The "clear" configuration resets statistics after gathering them\,
	 * where appropriate (for example\, a cache size statistic is not
	 * cleared\, while the count of cursor insert operations will be
	 * cleared). See @ref statistics for more information., a list\, with
	 * values chosen from the following options: \c "all"\, \c "fast"\, \c
	 * "clear"; default empty.}
	 * @config{target, if non-empty\, backup the list of objects; valid only
	 * for a backup data source., a list of strings; default empty.}
	 * @configend
	 * @param[out] dsp the data source handle
	 * @errors
	 */
	int __F(open_data_source)(WT_ASYNC_CONNECTION *connection,
	    const char *uri, const char *config, WT_ASYNC_DS **dsp);
};

/*!
 * See ::wiredtiger_open, this is the async equivalent.
 *
 * @param home The path to the database home directory.  See @ref home
 * for more information.
 * @param errhandler An error handler.  If <code>NULL</code>, a builtin error
 * handler is installed that writes error messages to stderr
 * @configempty{wiredtiger_async_open, see dist/api_data.py}
 * @configstart{wiredtiger_open, see dist/api_data.py}
 * @config{buffer_alignment, in-memory alignment (in bytes) for buffers used for
 * I/O. The default value of -1 indicates a platform-specific alignment value
 * should be used (4KB on Linux systems\, zero elsewhere)., an integer between
 * -1 and 1MB; default \c -1.}
 * @config{cache_size, maximum heap memory to allocate for the cache.  A
 * database should configure either a cache_size or a shared_cache not both., an
 * integer between 1MB and 10TB; default \c 100MB.}
 * @config{checkpoint = (, periodically checkpoint the database., a set of
 * related configuration options defined below.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;name, the checkpoint name., a string; default
 * \c "WiredTigerCheckpoint".}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;wait, seconds to
 * wait between each checkpoint; setting this value configures periodic
 * checkpoints., an integer between 1 and 100000; default \c 0.}
 * @config{ ),,}
 * @config{checkpoint_sync, flush files to stable storage when closing or
 * writing checkpoints., a boolean flag; default \c true.}
 * @config{create, create the database if it does not exist., a boolean flag;
 * default \c false.}
 * @config{direct_io, Use \c O_DIRECT to access files.  Options are given as a
 * list\, such as <code>"direct_io=[data]"</code>. Configuring \c direct_io
 * requires care\, see @ref tuning_system_buffer_cache_direct_io for important
 * warnings.  Including \c "data" will cause WiredTiger data files to use \c
 * O_DIRECT\, including \c "log" will cause WiredTiger log files to use \c
 * O_DIRECT\, and including \c "checkpoint" will cause WiredTiger data files
 * opened at a checkpoint (i.e: read only) to use \c O_DIRECT., a list\, with
 * values chosen from the following options: \c "checkpoint"\, \c "data"\, \c
 * "log"; default empty.}
 * @config{error_prefix, prefix string for error messages., a string; default
 * empty.}
 * @config{eviction_dirty_target, continue evicting until the cache has less
 * dirty pages than this (as a percentage). Dirty pages will only be evicted if
 * the cache is full enough to trigger eviction., an integer between 10 and 99;
 * default \c 80.}
 * @config{eviction_target, continue evicting until the cache becomes less full
 * than this (as a percentage). Must be less than \c eviction_trigger., an
 * integer between 10 and 99; default \c 80.}
 * @config{eviction_trigger, trigger eviction when the cache becomes this full
 * (as a percentage)., an integer between 10 and 99; default \c 95.}
 * @config{extensions, list of shared library extensions to load (using dlopen).
 * Any values specified to an library extension are passed to
 * WT_CONNECTION::load_extension as the \c config parameter (for example\,
 * <code>extensions=(/path/ext.so={entry=my_entry})</code>)., a list of strings;
 * default empty.}
 * @config{file_extend, file extension configuration.  If set\, extend files of
 * the set type in allocations of the set size\, instead of a block at a time as
 * each new block is written.  For example\,
 * <code>file_extend=(data=16MB)</code>., a list\, with values chosen from the
 * following options: \c "data"\, \c "log"; default empty.}
 * @config{hazard_max, maximum number of simultaneous hazard pointers per
 * session handle., an integer greater than or equal to 15; default \c 1000.}
 * @config{log = (, enable logging., a set of related configuration options
 * defined below.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;archive, automatically
 * archive unneeded log files., a boolean flag; default \c true.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;enabled, enable logging subsystem., a boolean
 * flag; default \c false.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;file_max, the
 * maximum size of log files., an integer between 100KB and 2GB; default \c
 * 100MB.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;path, the path to a directory into
 * which the log files are written.  If the value is not an absolute path name\,
 * the files are created relative to the database home., a string; default \c
 * "".}
 * @config{ ),,}
 * @config{lsm_merge, merge LSM chunks where possible., a boolean flag; default
 * \c true.}
 * @config{mmap, Use memory mapping to access files when possible., a boolean
 * flag; default \c true.}
 * @config{multiprocess, permit sharing between processes (will automatically
 * start an RPC server for primary processes and use RPC for secondary
 * processes). <b>Not yet supported in WiredTiger</b>., a boolean flag; default
 * \c false.}
 * @config{session_max, maximum expected number of sessions (including server
 * threads)., an integer greater than or equal to 1; default \c 100.}
 * @config{shared_cache = (, shared cache configuration options.  A database
 * should configure either a cache_size or a shared_cache not both., a set of
 * related configuration options defined below.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;enable, whether the connection is using a
 * shared cache., a boolean flag; default \c false.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;chunk, the granularity that a shared cache is
 * redistributed., an integer between 1MB and 10TB; default \c 10MB.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;reserve, amount of cache this database is
 * guaranteed to have available from the shared cache.  This setting is per
 * database.  Defaults to the chunk size., an integer; default \c 0.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;name, name of a cache that is shared between
 * databases., a string; default \c pool.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;size,
 * maximum memory to allocate for the shared cache.  Setting this will update
 * the value if one is already set., an integer between 1MB and 10TB; default \c
 * 500MB.}
 * @config{ ),,}
 * @config{statistics, Maintain database statistics\, which may impact
 * performance.  Choosing "all" maintains all statistics regardless of cost\,
 * "fast" maintains a subset of statistics that are relatively inexpensive\,
 * "none" turns off all statistics.  The "clear" configuration resets statistics
 * after they are gathered\, where appropriate (for example\, a cache size
 * statistic is not cleared\, while the count of cursor insert operations will
 * be cleared). When "clear" is configured for the database\, gathered
 * statistics are reset each time a statistics cursor is used to gather
 * statistics\, as well as each time statistics are logged using the \c
 * statistics_log configuration.  See @ref statistics for more information., a
 * list\, with values chosen from the following options: \c "all"\, \c "fast"\,
 * \c "none"\, \c "clear"; default \c none.}
 * @config{statistics_log = (, log any statistics the database is configured to
 * maintain\, to a file.  See @ref statistics for more information., a set of
 * related configuration options defined below.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;path, the pathname to a file into which the
 * log records are written\, may contain ISO C standard strftime conversion
 * specifications.  If the value is not an absolute path name\, the file is
 * created relative to the database home., a string; default \c
 * "WiredTigerStat.%d.%H".}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;sources, if
 * non-empty\, include statistics for the list of data source URIs\, if they are
 * open at the time of the statistics logging.  The list may include URIs
 * matching a single data source ("table:mytable")\, or a URI matching all data
 * sources of a particular type ("table:")., a list of strings; default empty.}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;timestamp, a timestamp prepended to each log
 * record\, may contain strftime conversion specifications., a string; default
 * \c "%b %d %H:%M:%S".}
 * @config{&nbsp;&nbsp;&nbsp;&nbsp;wait, seconds to wait
 * between each write of the log records., an integer between 1 and 100000;
 * default \c 0.}
 * @config{ ),,}
 * @config{transaction_sync, how to sync log records when the transaction
 * commits., a string\, chosen from the following options: \c "dsync"\, \c
 * "fsync"\, \c "none"; default \c fsync.}
 * @config{use_environment_priv, use the \c WIREDTIGER_CONFIG and \c
 * WIREDTIGER_HOME environment variables regardless of whether or not the
 * process is running with special privileges.  See @ref home for more
 * information., a boolean flag; default \c false.}
 * @config{verbose, enable messages for various events.  Options are given as a
 * list\, such as <code>"verbose=[evictserver\,read]"</code>., a list\, with
 * values chosen from the following options: \c "block"\, \c "ckpt"\, \c
 * "compact"\, \c "evict"\, \c "evictserver"\, \c "fileops"\, \c "log"\, \c
 * "lsm"\, \c "mutex"\, \c "overflow"\, \c "read"\, \c "readserver"\, \c
 * "reconcile"\, \c "recovery"\, \c "salvage"\, \c "shared_cache"\, \c
 * "verify"\, \c "version"\, \c "write"; default empty.}
 * @configend
 * @param[out] connectionp A pointer to the newly opened connection handle
 * @errors
 */
int wiredtiger_async_open(const char *home,
    WT_EVENT_HANDLER *errhandler, const char *config,
    WT_ASYNC_CONNECTION **connectionp);

/*!
 * @anchor error_returns
 * @name Error returns
 * @{
 */

/*! Stop an asynchronous scan.
 *
 * WT_ASYNC_CALLBACK::next returns this value to indicate than no further
 * records are required.
 */
#define	WT_ASYNC_SCAN_END	-31900

/*! @} */
/*! @} */

#undef __F

#if defined(__cplusplus)
}
#endif
#endif /* __WIREDTIGER_ASYNC_H_ */
