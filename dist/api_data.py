# This file is a python script that describes the WiredTiger API.

class Method:
    def __init__(self, config):
        self.config = config

class Config:
    def __init__(self, name, default, desc, subconfig=None, **flags):
        self.name = name
        self.default = default
        self.desc = desc
        self.subconfig = subconfig
        self.flags = flags

    def __cmp__(self, other):
        return cmp(self.name, other.name)

# Metadata shared by all schema objects
common_meta = [
    Config('app_metadata', '', r'''
        application-owned metadata for this object'''),
    Config('collator', 'none', r'''
        configure custom collation for keys.  Permitted values are \c "none"
        or a custom collator name created with WT_CONNECTION::add_collator'''),
    Config('columns', '', r'''
        list of the column names.  Comma-separated list of the form
        <code>(column[,...])</code>.  For tables, the number of entries
        must match the total number of values in \c key_format and \c
        value_format.  For colgroups and indices, all column names must
        appear in the list of columns for the table''',
        type='list'),
]

source_meta = [
    Config('source', '', r'''
        set a custom data source URI for a column group, index or simple
        table.  By default, the data source URI is derived from the \c
        type and the column group or index name.  Applications can
        create tables from existing data sources by supplying a \c
        source configuration''', undoc=True),
    Config('type', 'file', r'''
        set the type of data source used to store a column group, index
        or simple table.  By default, a \c "file:" URI is derived from
        the object name.  The \c type configuration can be used to
        switch to a different data source, such as LSM or an extension
        configured by the application'''),
]

format_meta = common_meta + [
    Config('key_format', 'u', r'''
        the format of the data packed into key items.  See @ref
        schema_format_types for details.  By default, the key_format is
        \c 'u' and applications use WT_ITEM structures to manipulate
        raw byte arrays. By default, records are stored in row-store
        files: keys of type \c 'r' are record numbers and records
        referenced by record number are stored in column-store files''',
        type='format', func='__wt_struct_confchk'),
    Config('value_format', 'u', r'''
        the format of the data packed into value items.  See @ref
        schema_format_types for details.  By default, the value_format
        is \c 'u' and applications use a WT_ITEM structure to
        manipulate raw byte arrays. Value items of type 't' are
        bitfields, and when configured with record number type keys,
        will be stored using a fixed-length store''',
        type='format', func='__wt_struct_confchk'),
]

lsm_config = [
    Config('lsm', '', r'''
        options only relevant for LSM data sources''',
        type='category', subconfig=[
        Config('auto_throttle', 'true', r'''
            Throttle inserts into LSM trees if flushing to disk isn't
            keeping up''',
            type='boolean'),
        Config('bloom', 'true', r'''
            create bloom filters on LSM tree chunks as they are merged''',
            type='boolean'),
        Config('bloom_bit_count', '16', r'''
            the number of bits used per item for LSM bloom filters''',
            min='2', max='1000'),
        Config('bloom_config', '', r'''
            config string used when creating Bloom filter files, passed
            to WT_SESSION::create'''),
        Config('bloom_hash_count', '8', r'''
            the number of hash values per item used for LSM bloom
            filters''',
            min='2', max='100'),
        Config('bloom_oldest', 'false', r'''
            create a bloom filter on the oldest LSM tree chunk. Only
            supported if bloom filters are enabled''',
            type='boolean'),
        Config('chunk_count_limit', '0', r'''
            the maximum number of chunks to allow in an LSM tree. This
            option automatically times out old data. As new chunks are
            added old chunks will be removed. Enabling this option
            disables LSM background merges''',
            type='int'),
        Config('chunk_max', '5GB', r'''
            the maximum size a single chunk can be. Chunks larger than this
            size are not considered for further merges. This is a soft
            limit, and chunks larger than this value can be created.  Must
            be larger than chunk_size''',
            min='100MB', max='10TB'),
        Config('chunk_size', '10MB', r'''
            the maximum size of the in-memory chunk of an LSM tree.  This
            limit is soft - it is possible for chunks to be temporarily
            larger than this value.  This overrides the \c memory_page_max
            setting''',
            min='512K', max='500MB'),
        Config('merge_max', '15', r'''
            the maximum number of chunks to include in a merge operation''',
            min='2', max='100'),
        Config('merge_min', '0', r'''
            the minimum number of chunks to include in a merge operation. If
            set to 0 or 1 half the value of merge_max is used''',
            max='100'),
    ]),
]

file_runtime_config = [
    Config('access_pattern_hint', 'none', r'''
        It is recommended that workloads that consist primarily of
        updates and/or point queries specify \c random.  Workloads that
        do many cursor scans through large ranges of data specify
        \c sequential and other workloads specify \c none.  The
        option leads to an advisory call to an appropriate operating
        system API where available''',
        choices=['none', 'random', 'sequential']),
    Config('cache_resident', 'false', r'''
        do not ever evict the object's pages from cache. Not compatible with
        LSM tables; see @ref tuning_cache_resident for more information''',
        type='boolean'),
    Config('log', '', r'''
        the transaction log configuration for this object.  Only valid if
        log is enabled in ::wiredtiger_open''',
        type='category', subconfig=[
        Config('enabled', 'true', r'''
            if false, this object has checkpoint-level durability''',
            type='boolean'),
        ]),
]

# Per-file configuration
file_config = format_meta + file_runtime_config + [
    Config('block_allocation', 'best', r'''
        configure block allocation. Permitted values are \c "first" or
        \c "best"; the \c "first" configuration uses a first-available
        algorithm during block allocation, the \c "best" configuration
        uses a best-fit algorithm''',
        choices=['first', 'best',]),
    Config('allocation_size', '4KB', r'''
        the file unit allocation size, in bytes, must a power-of-two;
        smaller values decrease the file space required by overflow
        items, and the default value of 4KB is a good choice absent
        requirements from the operating system or storage device''',
        min='512B', max='128MB'),
    Config('block_compressor', 'none', r'''
        configure a compressor for file blocks.  Permitted values are \c "none"
        or custom compression engine name created with
        WT_CONNECTION::add_compressor.  If WiredTiger has builtin support for
        \c "lz4", \c "snappy", \c "zlib" or \c "zstd" compression, these names
        are also available.  See @ref compression for more information'''),
    Config('checksum', 'uncompressed', r'''
        configure block checksums; permitted values are <code>on</code>
        (checksum all blocks), <code>off</code> (checksum no blocks) and
        <code>uncompresssed</code> (checksum only blocks which are not
        compressed for any reason).  The \c uncompressed setting is for
        applications which can rely on decompression to fail if a block
        has been corrupted''',
        choices=['on', 'off', 'uncompressed']),
    Config('dictionary', '0', r'''
        the maximum number of unique values remembered in the Btree
        row-store leaf page value dictionary; see
        @ref file_formats_compression for more information''',
        min='0'),
    Config('encryption', '', r'''
        configure an encryptor for file blocks. When a table is created,
        its encryptor is not implicitly used for any related indices
        or column groups''',
        type='category', subconfig=[
        Config('name', 'none', r'''
            Permitted values are \c "none"
            or custom encryption engine name created with
            WT_CONNECTION::add_encryptor.
            See @ref encryption for more information'''),
        Config('keyid', '', r'''
            An identifier that identifies a unique instance of the encryptor.
            It is stored in clear text, and thus is available when
            the wiredtiger database is reopened.  On the first use
            of a (name, keyid) combination, the WT_ENCRYPTOR::customize
            function is called with the keyid as an argument'''),
        ]),
    Config('format', 'btree', r'''
        the file format''',
        choices=['btree']),
    Config('huffman_key', 'none', r'''
        configure Huffman encoding for keys.  Permitted values are
        \c "none", \c "english", \c "utf8<file>" or \c "utf16<file>".
        See @ref huffman for more information'''),
    Config('huffman_value', 'none', r'''
        configure Huffman encoding for values.  Permitted values are
        \c "none", \c "english", \c "utf8<file>" or \c "utf16<file>".
        See @ref huffman for more information'''),
    Config('ignore_in_memory_cache_size', 'false', r'''
        allow update and insert operations to proceed even if the cache is
        already at capacity. Only valid in conjunction with in-memory
        databases. Should be used with caution - this configuration allows
        WiredTiger to consume memory over the configured cache limit''',
        type='boolean'),
    Config('internal_key_truncate', 'true', r'''
        configure internal key truncation, discarding unnecessary trailing
        bytes on internal keys (ignored for custom collators)''',
        type='boolean'),
    Config('internal_page_max', '4KB', r'''
        the maximum page size for internal nodes, in bytes; the size
        must be a multiple of the allocation size and is significant
        for applications wanting to avoid excessive L2 cache misses
        while searching the tree.  The page maximum is the bytes of
        uncompressed data, that is, the limit is applied before any
        block compression is done''',
        min='512B', max='512MB'),
    Config('internal_item_max', '0', r'''
        historic term for internal_key_max''',
        min=0, undoc=True),
    Config('internal_key_max', '0', r'''
        the largest key stored in an internal node, in bytes.  If set, keys
        larger than the specified size are stored as overflow items (which
        may require additional I/O to access).  The default and the maximum
        allowed value are both one-tenth the size of a newly split internal
        page''',
        min='0'),
    Config('key_gap', '10', r'''
        the maximum gap between instantiated keys in a Btree leaf page,
        constraining the number of keys processed to instantiate a
        random Btree leaf page key''',
        min='0', undoc=True),
    Config('leaf_key_max', '0', r'''
        the largest key stored in a leaf node, in bytes.  If set, keys
        larger than the specified size are stored as overflow items (which
        may require additional I/O to access).  The default value is
        one-tenth the size of a newly split leaf page''',
        min='0'),
    Config('leaf_page_max', '32KB', r'''
        the maximum page size for leaf nodes, in bytes; the size must
        be a multiple of the allocation size, and is significant for
        applications wanting to maximize sequential data transfer from
        a storage device.  The page maximum is the bytes of uncompressed
        data, that is, the limit is applied before any block compression
        is done''',
        min='512B', max='512MB'),
    Config('leaf_value_max', '0', r'''
        the largest value stored in a leaf node, in bytes.  If set, values
        larger than the specified size are stored as overflow items (which
        may require additional I/O to access). If the size is larger than
        the maximum leaf page size, the page size is temporarily ignored
        when large values are written. The default is one-half the size of
        a newly split leaf page''',
        min='0'),
    Config('leaf_item_max', '0', r'''
        historic term for leaf_key_max and leaf_value_max''',
        min=0, undoc=True),
    Config('memory_page_max', '5MB', r'''
        the maximum size a page can grow to in memory before being
        reconciled to disk.  The specified size will be adjusted to a lower
        bound of <code>leaf_page_max</code>, and an upper bound of
        <code>cache_size / 10</code>.  This limit is soft - it is possible
        for pages to be temporarily larger than this value.  This setting
        is ignored for LSM trees, see \c chunk_size''',
        min='512B', max='10TB'),
    Config('os_cache_max', '0', r'''
        maximum system buffer cache usage, in bytes.  If non-zero, evict
        object blocks from the system buffer cache after that many bytes
        from this object are read or written into the buffer cache''',
        min=0),
    Config('os_cache_dirty_max', '0', r'''
        maximum dirty system buffer cache usage, in bytes.  If non-zero,
        schedule writes for dirty blocks belonging to this object in the
        system buffer cache after that many bytes from this object are
        written into the buffer cache''',
        min=0),
    Config('prefix_compression', 'false', r'''
        configure prefix compression on row-store leaf pages''',
        type='boolean'),
    Config('prefix_compression_min', '4', r'''
        minimum gain before prefix compression will be used on row-store
        leaf pages''',
        min=0),
    Config('split_deepen_min_child', '0', r'''
        minimum entries in a page to consider deepening the tree. Pages
        will be considered for splitting and deepening the search tree
        as soon as there are more than the configured number of children
        ''',
        type='int', undoc=True),
    Config('split_deepen_per_child', '0', r'''
        entries allocated per child when deepening the tree''',
        type='int', undoc=True),
    Config('split_pct', '90', r'''
        the Btree page split size as a percentage of the maximum Btree
        page size, that is, when a Btree page is split, it will be
        split into smaller pages, where each page is the specified
        percentage of the maximum Btree page size''',
        min='50', max='100'),
]

# File metadata, including both configurable and non-configurable (internal)
file_meta = file_config + [
    Config('checkpoint', '', r'''
        the file checkpoint entries'''),
    Config('checkpoint_lsn', '', r'''
        LSN of the last checkpoint'''),
    Config('id', '', r'''
        the file's ID number'''),
    Config('version', '(major=0,minor=0)', r'''
        the file version'''),
]

lsm_meta = file_config + lsm_config + [
    Config('last', '', r'''
        the last allocated chunk ID'''),
    Config('chunks', '', r'''
        active chunks in the LSM tree'''),
    Config('old_chunks', '', r'''
        obsolete chunks in the LSM tree'''),
]

table_only_config = [
    Config('colgroups', '', r'''
        comma-separated list of names of column groups.  Each column
        group is stored separately, keyed by the primary key of the
        table.  If no column groups are specified, all columns are
        stored together in a single file.  All value columns in the
        table must appear in at least one column group.  Each column
        group must be created with a separate call to
        WT_SESSION::create''', type='list'),
]

index_only_config = [
    Config('extractor', 'none', r'''
        configure custom extractor for indices.  Permitted values are
        \c "none" or an extractor name created with
        WT_CONNECTION::add_extractor'''),
    Config('immutable', 'false', r'''
        configure the index to be immutable - that is an index is not changed
        by any update to a record in the table''', type='boolean'),
]

colgroup_meta = common_meta + source_meta

index_meta = format_meta + source_meta + index_only_config + [
    Config('index_key_columns', '', r'''
        number of public key columns''', type='int', undoc=True),
]

table_meta = format_meta + table_only_config

# Connection runtime config, shared by conn.reconfigure and wiredtiger_open
connection_runtime_config = [
    Config('async', '', r'''
        asynchronous operations configuration options''',
        type='category', subconfig=[
        Config('enabled', 'false', r'''
            enable asynchronous operation''',
            type='boolean'),
        Config('ops_max', '1024', r'''
            maximum number of expected simultaneous asynchronous
                operations''', min='1', max='4096'),
        Config('threads', '2', r'''
            the number of worker threads to service asynchronous requests.
            Each worker thread uses a session from the configured
            session_max''',
                min='1', max='20'), # !!! Must match WT_ASYNC_MAX_WORKERS
            ]),
    Config('cache_size', '100MB', r'''
        maximum heap memory to allocate for the cache. A database should
        configure either \c cache_size or \c shared_cache but not both''',
        min='1MB', max='10TB'),
    Config('cache_overhead', '8', r'''
        assume the heap allocator overhead is the specified percentage, and
        adjust the cache usage by that amount (for example, if there is 10GB
        of data in cache, a percentage of 10 means WiredTiger treats this as
        11GB).  This value is configurable because different heap allocators
        have different overhead and different workloads will have different
        heap allocation sizes and patterns, therefore applications may need to
        adjust this value based on allocator choice and behavior in measured
        workloads''',
        min='0', max='30'),
    Config('checkpoint', '', r'''
        periodically checkpoint the database. Enabling the checkpoint server
        uses a session from the configured session_max''',
        type='category', subconfig=[
        Config('log_size', '0', r'''
            wait for this amount of log record bytes to be written to
                the log between each checkpoint.  If non-zero, this value will
                use a minimum of the log file size.  A database can configure
                both log_size and wait to set an upper bound for checkpoints;
                setting this value above 0 configures periodic checkpoints''',
            min='0', max='2GB'),
        Config('wait', '0', r'''
            seconds to wait between each checkpoint; setting this value
            above 0 configures periodic checkpoints''',
            min='0', max='100000'),
        ]),
    Config('compatibility', '', r'''
        set compatibility version of database.  Changing the compatibility
        version requires that there are no active operations for the duration
        of the call.''',
        type='category', subconfig=[
        Config('release', '', r'''
            compatibility release version string'''),
        ]),
    Config('error_prefix', '', r'''
        prefix string for error messages'''),
    Config('eviction', '', r'''
        eviction configuration options''',
        type='category', subconfig=[
            Config('threads_max', '8', r'''
                maximum number of threads WiredTiger will start to help evict
                pages from cache. The number of threads started will vary
                depending on the current eviction load. Each eviction worker
                thread uses a session from the configured session_max''',
                min=1, max=20),
            Config('threads_min', '1', r'''
                minimum number of threads WiredTiger will start to help evict
                pages from cache. The number of threads currently running will
                vary depending on the current eviction load''',
                min=1, max=20),
            ]),
    Config('eviction_checkpoint_target', '5', r'''
        perform eviction at the beginning of checkpoints to bring the dirty
        content in cache to this level, expressed as a percentage of the total
        cache size.  Ignored if set to zero or \c in_memory is \c true''',
        min=0, max=99),
    Config('eviction_dirty_target', '5', r'''
        perform eviction in worker threads when the cache contains at least
        this much dirty content, expressed as a percentage of the total cache
        size.''',
        min=1, max=99),
    Config('eviction_dirty_trigger', '20', r'''
        trigger application threads to perform eviction when the cache contains
        at least this much dirty content, expressed as a percentage of the
        total cache size. This setting only alters behavior if it is lower than
        eviction_trigger''',
        min=1, max=99),
    Config('eviction_target', '80', r'''
        perform eviction in worker threads when the cache contains at least
        this much content, expressed as a percentage of the total cache size.
        Must be less than \c eviction_trigger''',
        min=10, max=99),
    Config('eviction_trigger', '95', r'''
        trigger application threads to perform eviction when the cache contains
        at least this much content, expressed as a percentage of the
        total cache size''', min=10, max=99),
    Config('file_manager', '', r'''
        control how file handles are managed''',
        type='category', subconfig=[
        Config('close_handle_minimum', '250', r'''
            number of handles open before the file manager will look for handles
            to close''', min=0),
        Config('close_idle_time', '30', r'''
            amount of time in seconds a file handle needs to be idle
            before attempting to close it. A setting of 0 means that idle
            handles are not closed''', min=0, max=100000),
        Config('close_scan_interval', '10', r'''
            interval in seconds at which to check for files that are
            inactive and close them''', min=1, max=100000),
        ]),
    Config('lsm_manager', '', r'''
        configure database wide options for LSM tree management. The LSM
        manager is started automatically the first time an LSM tree is opened.
        The LSM manager uses a session from the configured session_max''',
        type='category', subconfig=[
        Config('worker_thread_max', '4', r'''
            Configure a set of threads to manage merging LSM trees in
            the database. Each worker thread uses a session handle from
            the configured session_max''',
            min='3',     # !!! Must match WT_LSM_MIN_WORKERS
            max='20'),     # !!! Must match WT_LSM_MAX_WORKERS
        Config('merge', 'true', r'''
            merge LSM chunks where possible''',
            type='boolean')
        ]),
    Config('lsm_merge', 'true', r'''
        merge LSM chunks where possible (deprecated)''',
        type='boolean', undoc=True),
    Config('shared_cache', '', r'''
        shared cache configuration options. A database should configure
        either a cache_size or a shared_cache not both. Enabling a
        shared cache uses a session from the configured session_max''',
        type='category', subconfig=[
        Config('chunk', '10MB', r'''
            the granularity that a shared cache is redistributed''',
            min='1MB', max='10TB'),
        Config('name', 'none', r'''
            the name of a cache that is shared between databases or
            \c "none" when no shared cache is configured'''),
        Config('quota', '0', r'''
            maximum size of cache this database can be allocated from the
            shared cache. Defaults to the entire shared cache size''',
            type='int'),
        Config('reserve', '0', r'''
            amount of cache this database is guaranteed to have
            available from the shared cache. This setting is per
            database. Defaults to the chunk size''', type='int'),
        Config('size', '500MB', r'''
            maximum memory to allocate for the shared cache. Setting
            this will update the value if one is already set''',
            min='1MB', max='10TB')
        ]),
    Config('statistics', 'none', r'''
        Maintain database statistics, which may impact performance.
        Choosing "all" maintains all statistics regardless of cost,
        "fast" maintains a subset of statistics that are relatively
        inexpensive, "none" turns off all statistics.  The "clear"
        configuration resets statistics after they are gathered,
        where appropriate (for example, a cache size statistic is
        not cleared, while the count of cursor insert operations will
        be cleared).   When "clear" is configured for the database,
        gathered statistics are reset each time a statistics cursor
        is used to gather statistics, as well as each time statistics
        are logged using the \c statistics_log configuration.  See
        @ref statistics for more information''',
        type='list',
        choices=['all', 'cache_walk', 'fast', 'none', 'clear', 'tree_walk']),
    Config('timing_stress_for_test', '', r'''
        enable code that interrupts the usual timing of operations with a
        goal of uncovering race conditions and unexpected blocking.
        This option is intended for use with internal stress
        testing of WiredTiger. Options are given as a list, such as
        <code>"timing_stress_for_test=[checkpoint_slow,
            internal_page_split_race, page_split_race]"</code>''',
        type='list', undoc=True, choices=[
            'checkpoint_slow', 'internal_page_split_race', 'page_split_race']),
    Config('verbose', '', r'''
        enable messages for various events. Only available if WiredTiger
        is configured with --enable-verbose. Options are given as a
        list, such as <code>"verbose=[evictserver,read]"</code>''',
        type='list', choices=[
            'api',
            'block',
            'checkpoint',
            'compact',
            'evict',
            'evict_stuck',
            'evictserver',
            'fileops',
            'handleops',
            'log',
            'lookaside_activity',
            'lsm',
            'lsm_manager',
            'metadata',
            'mutex',
            'overflow',
            'read',
            'rebalance',
            'reconcile',
            'recovery',
            'recovery_progress',
            'salvage',
            'shared_cache',
            'split',
            'temporary',
            'thread_group',
            'timestamp',
            'transaction',
            'verify',
            'version',
            'write']),
]

# wiredtiger_open and WT_CONNECTION.reconfigure log configurations.
log_configuration_common = [
    Config('archive', 'true', r'''
        automatically archive unneeded log files''',
        type='boolean'),
    Config('prealloc', 'true', r'''
        pre-allocate log files''',
        type='boolean'),
    Config('zero_fill', 'false', r'''
        manually write zeroes into log files''',
        type='boolean')
]
connection_reconfigure_log_configuration = [
    Config('log', '', r'''
        enable logging. Enabling logging uses three sessions from the
        configured session_max''',
        type='category', subconfig=
        log_configuration_common)
]
wiredtiger_open_log_configuration = [
    Config('log', '', r'''
        enable logging. Enabling logging uses three sessions from the
        configured session_max''',
        type='category', subconfig=
        log_configuration_common + [
        Config('enabled', 'false', r'''
            enable logging subsystem''',
            type='boolean'),
        Config('compressor', 'none', r'''
            configure a compressor for log records.  Permitted values are
            \c "none" or custom compression engine name created with
            WT_CONNECTION::add_compressor.  If WiredTiger has builtin support
            for \c "lz4", \c "snappy", \c "zlib" or \c "zstd" compression,
            these names are also available. See @ref compression for more
            information'''),
        Config('file_max', '100MB', r'''
            the maximum size of log files''',
            min='100KB', max='2GB'),
            Config('path', '"."', r'''
                the name of a directory into which log files are written. The
                directory must already exist. If the value is not an absolute
                path, the path is relative to the database home (see @ref
                absolute_path for more information)'''),
        Config('recover', 'on', r'''
            run recovery or error if recovery needs to run after an
            unclean shutdown''',
            choices=['error','on'])
    ]),
]

# wiredtiger_open and WT_CONNECTION.reconfigure statistics log configurations.
statistics_log_configuration_common = [
    Config('json', 'false', r'''
        encode statistics in JSON format''',
        type='boolean'),
    Config('on_close', 'false', r'''log statistics on database close''',
        type='boolean'),
    Config('sources', '', r'''
        if non-empty, include statistics for the list of data source
        URIs, if they are open at the time of the statistics logging.
        The list may include URIs matching a single data source
        ("table:mytable"), or a URI matching all data sources of a
        particular type ("table:")''',
        type='list'),
    Config('timestamp', '"%b %d %H:%M:%S"', r'''
        a timestamp prepended to each log record, may contain strftime
        conversion specifications, when \c json is configured, defaults
        to \c "%FT%Y.000Z"'''),
    Config('wait', '0', r'''
        seconds to wait between each write of the log records; setting
        this value above 0 configures statistics logging''',
        min='0', max='100000'),
]
connection_reconfigure_statistics_log_configuration = [
    Config('statistics_log', '', r'''
        log any statistics the database is configured to maintain,
        to a file.  See @ref statistics for more information. Enabling
        the statistics log server uses a session from the configured
        session_max''',
        type='category', subconfig=
        statistics_log_configuration_common)
]
wiredtiger_open_statistics_log_configuration = [
    Config('statistics_log', '', r'''
        log any statistics the database is configured to maintain,
        to a file.  See @ref statistics for more information. Enabling
        the statistics log server uses a session from the configured
        session_max''',
        type='category', subconfig=
        statistics_log_configuration_common + [
        Config('path', '"."', r'''
            the name of a directory into which statistics files are written.
            The directory must already exist. If the value is not an absolute
            path, the path is relative to the database home (see @ref
            absolute_path for more information)''')
        ])
]

session_config = [
    Config('ignore_cache_size', 'false', r'''
        when set, operations performed by this session ignore the cache size
        and are not blocked when the cache is full.  Note that use of this
        option for operations that create cache pressure can starve ordinary
        sessions that obey the cache size.''',
        type='boolean'),
    Config('isolation', 'read-committed', r'''
        the default isolation level for operations in this session''',
        choices=['read-uncommitted', 'read-committed', 'snapshot']),
]

wiredtiger_open_common =\
    connection_runtime_config +\
    wiredtiger_open_log_configuration +\
    wiredtiger_open_statistics_log_configuration + [
    Config('buffer_alignment', '-1', r'''
        in-memory alignment (in bytes) for buffers used for I/O.  The
        default value of -1 indicates a platform-specific alignment value
        should be used (4KB on Linux systems when direct I/O is configured,
        zero elsewhere)''',
        min='-1', max='1MB'),
    Config('builtin_extension_config', '', r'''
        A structure where the keys are the names of builtin extensions and the
        values are passed to WT_CONNECTION::load_extension as the \c config
        parameter (for example,
        <code>builtin_extension_config={zlib={compression_level=3}}</code>)'''),
    Config('checkpoint_sync', 'true', r'''
        flush files to stable storage when closing or writing
        checkpoints''',
        type='boolean'),
    Config('direct_io', '', r'''
        Use \c O_DIRECT on POSIX systems, and \c FILE_FLAG_NO_BUFFERING on
        Windows to access files.  Options are given as a list, such as
        <code>"direct_io=[data]"</code>.  Configuring \c direct_io requires
        care, see @ref tuning_system_buffer_cache_direct_io for important
        warnings.  Including \c "data" will cause WiredTiger data files to use
        direct I/O, including \c "log" will cause WiredTiger log files to use
        direct I/O, and including \c "checkpoint" will cause WiredTiger data
        files opened at a checkpoint (i.e: read only) to use direct I/O.
        \c direct_io should be combined with \c write_through to get the
        equivalent of \c O_DIRECT on Windows''',
        type='list', choices=['checkpoint', 'data', 'log']),
    Config('encryption', '', r'''
        configure an encryptor for system wide metadata and logs.
        If a system wide encryptor is set, it is also used for
        encrypting data files and tables, unless encryption configuration
        is explicitly set for them when they are created with
        WT_SESSION::create''',
        type='category', subconfig=[
        Config('name', 'none', r'''
            Permitted values are \c "none"
            or custom encryption engine name created with
            WT_CONNECTION::add_encryptor.
            See @ref encryption for more information'''),
        Config('keyid', '', r'''
            An identifier that identifies a unique instance of the encryptor.
            It is stored in clear text, and thus is available when
            the wiredtiger database is reopened.  On the first use
            of a (name, keyid) combination, the WT_ENCRYPTOR::customize
            function is called with the keyid as an argument'''),
        Config('secretkey', '', r'''
            A string that is passed to the WT_ENCRYPTOR::customize function.
            It is never stored in clear text, so must be given to any
            subsequent ::wiredtiger_open calls to reopen the database.
            It must also be provided to any "wt" commands used with
            this database'''),
        ]),
    Config('extensions', '', r'''
        list of shared library extensions to load (using dlopen).
        Any values specified to a library extension are passed to
        WT_CONNECTION::load_extension as the \c config parameter
        (for example,
        <code>extensions=(/path/ext.so={entry=my_entry})</code>)''',
        type='list'),
    Config('file_extend', '', r'''
        file extension configuration.  If set, extend files of the set
        type in allocations of the set size, instead of a block at a
        time as each new block is written.  For example,
        <code>file_extend=(data=16MB)</code>''',
        type='list', choices=['data', 'log']),
    Config('hazard_max', '1000', r'''
        maximum number of simultaneous hazard pointers per session
        handle''',
        min=15, undoc=True),
    Config('mmap', 'true', r'''
        Use memory mapping to access files when possible''',
        type='boolean'),
    Config('multiprocess', 'false', r'''
        permit sharing between processes (will automatically start an
        RPC server for primary processes and use RPC for secondary
        processes). <b>Not yet supported in WiredTiger</b>''',
        type='boolean'),
    Config('readonly', 'false', r'''
        open connection in read-only mode.  The database must exist.  All
        methods that may modify a database are disabled.  See @ref readonly
        for more information''',
        type='boolean'),
    Config('session_max', '100', r'''
        maximum expected number of sessions (including server
        threads)''',
        min='1'),
    Config('session_scratch_max', '2MB', r'''
        maximum memory to cache in each session''',
        type='int', undoc=True),
    Config('transaction_sync', '', r'''
        how to sync log records when the transaction commits''',
        type='category', subconfig=[
        Config('enabled', 'false', r'''
            whether to sync the log on every commit by default, can be
            overridden by the \c sync setting to
            WT_SESSION::commit_transaction''',
            type='boolean'),
        Config('method', 'fsync', r'''
            the method used to ensure log records are stable on disk, see
            @ref tune_durability for more information''',
            choices=['dsync', 'fsync', 'none']),
        ]),
    Config('write_through', '', r'''
        Use \c FILE_FLAG_WRITE_THROUGH on Windows to write to files.  Ignored
        on non-Windows systems.  Options are given as a list, such as
        <code>"write_through=[data]"</code>.  Configuring \c write_through
        requires care, see @ref tuning_system_buffer_cache_direct_io for
        important warnings.  Including \c "data" will cause WiredTiger data
        files to write through cache, including \c "log" will cause WiredTiger
        log files to write through cache. \c write_through should be combined
        with \c direct_io to get the equivalent of POSIX \c O_DIRECT on
        Windows''',
        type='list', choices=['data', 'log']),
]

wiredtiger_open = wiredtiger_open_common + [
   Config('config_base', 'true', r'''
        write the base configuration file if creating the database.  If
        \c false in the config passed directly to ::wiredtiger_open, will
        ignore any existing base configuration file in addition to not creating
        one.  See @ref config_base for more information''',
        type='boolean'),
    Config('create', 'false', r'''
        create the database if it does not exist''',
        type='boolean'),
    Config('exclusive', 'false', r'''
        fail if the database already exists, generally used with the
        \c create option''',
        type='boolean'),
    Config('in_memory', 'false', r'''
        keep data in-memory only. See @ref in_memory for more information''',
        type='boolean'),
    Config('use_environment', 'true', r'''
        use the \c WIREDTIGER_CONFIG and \c WIREDTIGER_HOME environment
        variables if the process is not running with special privileges.
        See @ref home for more information''',
        type='boolean'),
    Config('use_environment_priv', 'false', r'''
        use the \c WIREDTIGER_CONFIG and \c WIREDTIGER_HOME environment
        variables even if the process is running with special privileges.
        See @ref home for more information''',
        type='boolean'),
]

cursor_runtime_config = [
    Config('append', 'false', r'''
        append the value as a new record, creating a new record
        number key; valid only for cursors with record number keys''',
        type='boolean'),
    Config('overwrite', 'true', r'''
        configures whether the cursor's insert, update and remove
        methods check the existing state of the record.  If \c overwrite
        is \c false, WT_CURSOR::insert fails with ::WT_DUPLICATE_KEY
        if the record exists, WT_CURSOR::update and WT_CURSOR::remove
        fail with ::WT_NOTFOUND if the record does not exist''',
        type='boolean'),
]

methods = {
'colgroup.meta' : Method(colgroup_meta),

'file.config' : Method(file_config),

'file.meta' : Method(file_meta),

'index.meta' : Method(index_meta),

'lsm.meta' : Method(lsm_meta),

'table.meta' : Method(table_meta),

'WT_CURSOR.close' : Method([]),

'WT_CURSOR.reconfigure' : Method(cursor_runtime_config),

'WT_SESSION.alter' : Method(file_runtime_config),

'WT_SESSION.close' : Method([]),

'WT_SESSION.compact' : Method([
    Config('timeout', '1200', r'''
        maximum amount of time to allow for compact in seconds. The
        actual amount of time spent in compact may exceed the configured
        value. A value of zero disables the timeout''',
        type='int'),
]),

'WT_SESSION.create' : Method(file_config + lsm_config + source_meta +
        index_only_config + table_only_config + [
    Config('exclusive', 'false', r'''
        fail if the object exists.  When false (the default), if the
        object exists, check that its settings match the specified
        configuration''',
        type='boolean'),
]),

'WT_SESSION.drop' : Method([
    Config('checkpoint_wait', 'true', r'''
        wait for concurrent checkpoints to complete before attempting the drop
        operation. If \c checkpoint_wait=false, attempt the drop operation
        without waiting, returning EBUSY if the operation conflicts with a
        running checkpoint''',
        type='boolean', undoc=True),
    Config('force', 'false', r'''
        return success if the object does not exist''',
        type='boolean'),
    Config('lock_wait', 'true', r'''
        wait for locks, if \c lock_wait=false, fail if any required locks are
        not available immediately''',
        type='boolean', undoc=True),
    Config('remove_files', 'true', r'''
        if the underlying files should be removed''',
        type='boolean'),
]),

'WT_SESSION.join' : Method([
    Config('compare', '"eq"', r'''
        modifies the set of items to be returned so that the index key
        satisfies the given comparison relative to the key set in this
        cursor''',
        choices=['eq', 'ge', 'gt', 'le', 'lt']),
    Config('count', '', r'''
        set an approximate count of the elements that would be included in
        the join.  This is used in sizing the bloom filter, and also influences
        evaluation order for cursors in the join. When the count is equal
        for multiple bloom filters in a composition of joins, the bloom
        filter may be shared''',
        type='int'),
    Config('bloom_bit_count', '16', r'''
        the number of bits used per item for the bloom filter''',
        min='2', max='1000'),
    Config('bloom_false_positives', 'false', r'''
        return all values that pass the bloom filter, without eliminating
        any false positives''',
        type='boolean'),
    Config('bloom_hash_count', '8', r'''
        the number of hash values per item for the bloom filter''',
        min='2', max='100'),
    Config('operation', '"and"', r'''
        the operation applied between this and other joined cursors.
        When "operation=and" is specified, all the conditions implied by
        joins must be satisfied for an entry to be returned by the join cursor;
        when "operation=or" is specified, only one must be satisfied.
        All cursors joined to a join cursor must have matching operations''',
        choices=['and', 'or']),
    Config('strategy', '', r'''
        when set to bloom, a bloom filter is created and populated for
        this index. This has an up front cost but may reduce the number
        of accesses to the main table when iterating the joined cursor.
        The bloom setting requires that count be set''',
        choices=['bloom', 'default']),
]),

'WT_SESSION.log_flush' : Method([
    Config('sync', 'on', r'''
        forcibly flush the log and wait for it to achieve the synchronization
        level specified.  The \c background setting initiates a background
        synchronization intended to be used with a later call to
        WT_SESSION::transaction_sync.  The \c off setting forces any
        buffered log records to be written to the file system.  The
        \c on setting forces log records to be written to the storage device''',
        choices=['background', 'off', 'on']),
]),

'WT_SESSION.log_printf' : Method([]),

'WT_SESSION.open_cursor' : Method(cursor_runtime_config + [
    Config('bulk', 'false', r'''
        configure the cursor for bulk-loading, a fast, initial load path
        (see @ref tune_bulk_load for more information).  Bulk-load may
        only be used for newly created objects and applications should
        use the WT_CURSOR::insert method to insert rows.  When
        bulk-loading, rows must be loaded in sorted order.  The value
        is usually a true/false flag; when bulk-loading fixed-length
        column store objects, the special value \c bitmap allows chunks
        of a memory resident bitmap to be loaded directly into a file
        by passing a \c WT_ITEM to WT_CURSOR::set_value where the \c
        size field indicates the number of records in the bitmap (as
        specified by the object's \c value_format configuration).
        Bulk-loaded bitmap values must end on a byte boundary relative
        to the bit count (except for the last set of values loaded)'''),
    Config('checkpoint', '', r'''
        the name of a checkpoint to open (the reserved name
        "WiredTigerCheckpoint" opens the most recent internal
        checkpoint taken for the object).  The cursor does not
        support data modification'''),
    Config('checkpoint_wait', 'true', r'''
        wait for the checkpoint lock, if \c checkpoint_wait=false, open the
        cursor without taking a lock, returning EBUSY if the operation
        conflicts with a running checkpoint''',
        type='boolean', undoc=True),
    Config('dump', '', r'''
        configure the cursor for dump format inputs and outputs: "hex"
        selects a simple hexadecimal format, "json" selects a JSON format
        with each record formatted as fields named by column names if
        available, and "print" selects a format where only non-printing
        characters are hexadecimal encoded.  These formats are compatible
        with the @ref util_dump and @ref util_load commands''',
        choices=['hex', 'json', 'print']),
    Config('next_random', 'false', r'''
        configure the cursor to return a pseudo-random record from the
        object when the WT_CURSOR::next method is called; valid only for
        row-store cursors. See @ref cursor_random for details''',
        type='boolean'),
    Config('next_random_sample_size', '0', r'''
        cursors configured by \c next_random to return pseudo-random
        records from the object randomly select from the entire object,
        by default. Setting \c next_random_sample_size to a non-zero
        value sets the number of samples the application expects to take
        using the \c next_random cursor. A cursor configured with both
        \c next_random and \c next_random_sample_size attempts to divide
        the object into \c next_random_sample_size equal-sized pieces,
        and each retrieval returns a record from one of those pieces. See
        @ref cursor_random for details'''),
    Config('raw', 'false', r'''
        ignore the encodings for the key and value, manage data as if
        the formats were \c "u".  See @ref cursor_raw for details''',
        type='boolean'),
    Config('readonly', 'false', r'''
        only query operations are supported by this cursor. An error is
        returned if a modification is attempted using the cursor.  The
        default is false for all cursor types except for log and metadata
        cursors''',
        type='boolean'),
    Config('skip_sort_check', 'false', r'''
        skip the check of the sort order of each bulk-loaded key''',
        type='boolean', undoc=True),
    Config('statistics', '', r'''
        Specify the statistics to be gathered.  Choosing "all" gathers
        statistics regardless of cost and may include traversing on-disk files;
        "fast" gathers a subset of relatively inexpensive statistics.  The
        selection must agree with the database \c statistics configuration
        specified to ::wiredtiger_open or WT_CONNECTION::reconfigure.  For
        example, "all" or "fast" can be configured when the database is
        configured with "all", but the cursor open will fail if "all" is
        specified when the database is configured with "fast", and the cursor
        open will fail in all cases when the database is configured with
        "none".  If "size" is configured, only the underlying size of the
        object on disk is filled in and the object is not opened.  If \c
        statistics is not configured, the default configuration is the database
        configuration.  The "clear" configuration resets statistics after
        gathering them, where appropriate (for example, a cache size statistic
        is not cleared, while the count of cursor insert operations will be
        cleared).  See @ref statistics for more information''',
        type='list',
        choices=['all', 'cache_walk', 'fast', 'clear', 'size', 'tree_walk']),
    Config('target', '', r'''
        if non-empty, backup the list of objects; valid only for a
        backup data source''',
        type='list'),
]),

'WT_SESSION.rebalance' : Method([]),
'WT_SESSION.rename' : Method([]),
'WT_SESSION.reset' : Method([]),
'WT_SESSION.salvage' : Method([
    Config('force', 'false', r'''
        force salvage even of files that do not appear to be WiredTiger
        files''',
        type='boolean'),
]),
'WT_SESSION.strerror' : Method([]),
'WT_SESSION.transaction_sync' : Method([
    Config('timeout_ms', '1200000', # !!! Must match WT_SESSION_BG_SYNC_MSEC
        r'''
        maximum amount of time to wait for background sync to complete in
        milliseconds.  A value of zero disables the timeout and returns
        immediately''',
        type='int'),
]),

'WT_SESSION.truncate' : Method([]),
'WT_SESSION.upgrade' : Method([]),
'WT_SESSION.verify' : Method([
    Config('dump_address', 'false', r'''
        Display addresses and page types as pages are verified,
        using the application's message handler, intended for debugging''',
        type='boolean'),
    Config('dump_blocks', 'false', r'''
        Display the contents of on-disk blocks as they are verified,
        using the application's message handler, intended for debugging''',
        type='boolean'),
    Config('dump_layout', 'false', r'''
        Display the layout of the files as they are verified, using the
        application's message handler, intended for debugging; requires
        optional support from the block manager''',
        type='boolean'),
    Config('dump_offsets', '', r'''
        Display the contents of specific on-disk blocks,
        using the application's message handler, intended for debugging''',
        type='list'),
    Config('dump_pages', 'false', r'''
        Display the contents of in-memory pages as they are verified,
        using the application's message handler, intended for debugging''',
        type='boolean'),
    Config('strict', 'false', r'''
        Treat any verification problem as an error; by default, verify will
        warn, but not fail, in the case of errors that won't affect future
        behavior (for example, a leaked block)''',
        type='boolean')
]),

'WT_SESSION.begin_transaction' : Method([
    Config('isolation', '', r'''
        the isolation level for this transaction; defaults to the
        session's isolation level''',
        choices=['read-uncommitted', 'read-committed', 'snapshot']),
    Config('name', '', r'''
        name of the transaction for tracing and debugging'''),
    Config('priority', 0, r'''
        priority of the transaction for resolving conflicts.
        Transactions with higher values are less likely to abort''',
        min='-100', max='100'),
    Config('read_timestamp', '', r'''
        read using the specified timestamp, see
        @ref transaction_timestamps'''),
    Config('snapshot', '', r'''
        use a named, in-memory snapshot, see
        @ref transaction_named_snapshots'''),
    Config('sync', '', r'''
        whether to sync log records when the transaction commits,
        inherited from ::wiredtiger_open \c transaction_sync''',
        type='boolean'),
]),

'WT_SESSION.commit_transaction' : Method([
    Config('commit_timestamp', '', r'''
        set the commit timestamp for the current transaction, see
        @ref transaction_timestamps'''),
    Config('sync', '', r'''
        override whether to sync log records when the transaction commits,
        inherited from ::wiredtiger_open \c transaction_sync.
        The \c background setting initiates a background
        synchronization intended to be used with a later call to
        WT_SESSION::transaction_sync.  The \c off setting does not
        wait for record to be written or synchronized.  The
        \c on setting forces log records to be written to the storage device''',
        choices=['background', 'off', 'on']),
]),

'WT_SESSION.timestamp_transaction' : Method([
    Config('commit_timestamp', '', r'''
        set the commit timestamp for the current transaction, see
        @ref transaction_timestamps'''),
]),

'WT_SESSION.rollback_transaction' : Method([]),

'WT_SESSION.checkpoint' : Method([
    Config('drop', '', r'''
        specify a list of checkpoints to drop.
        The list may additionally contain one of the following keys:
        \c "from=all" to drop all checkpoints,
        \c "from=<checkpoint>" to drop all checkpoints after and
        including the named checkpoint, or
        \c "to=<checkpoint>" to drop all checkpoints before and
        including the named checkpoint.  Checkpoints cannot be
        dropped while a hot backup is in progress or if open in
        a cursor''', type='list'),
    Config('force', 'false', r'''
        by default, checkpoints may be skipped if the underlying object
        has not been modified, this option forces the checkpoint''',
        type='boolean'),
    Config('name', '', r'''
        if set, specify a name for the checkpoint (note that checkpoints
        including LSM trees may not be named)'''),
    Config('target', '', r'''
        if non-empty, checkpoint the list of objects''', type='list'),
    Config('use_timestamp', 'true', r'''
        by default, create the checkpoint as of the last stable timestamp
        if timestamps are in use, or all current updates if there is no
        stable timestamp set.  If false, this option generates a checkpoint
        with all updates including those later than the timestamp''',
        type='boolean'),
]),

'WT_SESSION.snapshot' : Method([
    Config('drop', '', r'''
            if non-empty, specifies which snapshots to drop. Where a group
            of snapshots are being dropped, the order is based on snapshot
            creation order not alphanumeric name order''',
        type='category', subconfig=[
        Config('all', 'false', r'''
            drop all named snapshots''', type='boolean'),
        Config('before', '', r'''
            drop all snapshots up to but not including the specified name'''),
        Config('names', '', r'''
            drop specific named snapshots''', type='list'),
        Config('to', '', r'''
            drop all snapshots up to and including the specified name'''),
    ]),
    Config('include_updates', 'false', r'''
        make updates from the current transaction visible to users of the
        named snapshot.  Transactions started with such a named snapshot are
        restricted to being read-only''', type='boolean'),
    Config('name', '', r'''specify a name for the snapshot'''),
]),

'WT_CONNECTION.add_collator' : Method([]),
'WT_CONNECTION.add_compressor' : Method([]),
'WT_CONNECTION.add_data_source' : Method([]),
'WT_CONNECTION.add_encryptor' : Method([]),
'WT_CONNECTION.add_extractor' : Method([]),
'WT_CONNECTION.async_new_op' : Method([
    Config('append', 'false', r'''
        append the value as a new record, creating a new record
        number key; valid only for operations with record number keys''',
        type='boolean'),
    Config('overwrite', 'true', r'''
        configures whether the cursor's insert, update and remove
        methods check the existing state of the record.  If \c overwrite
        is \c false, WT_CURSOR::insert fails with ::WT_DUPLICATE_KEY
        if the record exists, WT_CURSOR::update and WT_CURSOR::remove
        fail with ::WT_NOTFOUND if the record does not exist''',
        type='boolean'),
    Config('raw', 'false', r'''
        ignore the encodings for the key and value, manage data as if
        the formats were \c "u".  See @ref cursor_raw for details''',
        type='boolean'),
    Config('timeout', '1200', r'''
        maximum amount of time to allow for compact in seconds. The
        actual amount of time spent in compact may exceed the configured
        value. A value of zero disables the timeout''',
        type='int'),
]),
'WT_CONNECTION.close' : Method([
    Config('leak_memory', 'false', r'''
        don't free memory during close''',
        type='boolean'),
]),
'WT_CONNECTION.debug_info' : Method([
    Config('cache', 'false', r'''
        print cache information''', type='boolean'),
    Config('cursors', 'false', r'''
        print all open cursor information''', type='boolean'),
    Config('handles', 'false', r'''
        print open handles information''', type='boolean'),
    Config('log', 'false', r'''
        print log information''', type='boolean'),
    Config('sessions', 'false', r'''
        print open session information''', type='boolean'),
    Config('txn', 'false', r'''
        print global txn information''', type='boolean'),
]),
'WT_CONNECTION.reconfigure' : Method(
    connection_reconfigure_log_configuration +\
    connection_reconfigure_statistics_log_configuration +\
    connection_runtime_config
),
'WT_CONNECTION.set_file_system' : Method([]),

'WT_CONNECTION.load_extension' : Method([
    Config('config', '', r'''
        configuration string passed to the entry point of the
        extension as its WT_CONFIG_ARG argument'''),
    Config('early_load', 'false', r'''
        whether this extension should be loaded at the beginning of
        ::wiredtiger_open. Only applicable to extensions loaded via the
        wiredtiger_open configurations string''',
        type='boolean'),
    Config('entry', 'wiredtiger_extension_init', r'''
        the entry point of the extension, called to initialize the
        extension when it is loaded.  The signature of the function
        must match ::wiredtiger_extension_init'''),
    Config('terminate', 'wiredtiger_extension_terminate', r'''
        an optional function in the extension that is called before
        the extension is unloaded during WT_CONNECTION::close.  The
        signature of the function must match
        ::wiredtiger_extension_terminate'''),
]),

'WT_CONNECTION.open_session' : Method(session_config),

'WT_CONNECTION.query_timestamp' : Method([
    Config('get', 'all_committed', r'''
        specify which timestamp to query: \c all_committed returns the largest
        timestamp such that all earlier timestamps have committed, \c oldest
        returns the most recent \c oldest_timestamp set with
        WT_CONNECTION::set_timestamp, \c pinned returns the minimum of the
        \c oldest_timestamp and the read timestamps of all active readers, and
        \c stable returns the most recent \c stable_timestamp set with
        WT_CONNECTION::set_timestamp.  See @ref transaction_timestamps''',
        choices=['all_committed','oldest','pinned','stable']),
]),

'WT_CONNECTION.set_timestamp' : Method([
    Config('commit_timestamp', '', r'''
        reset the maximum commit timestamp tracked by WiredTiger.  This will
        cause future calls to WT_CONNECTION::query_timestamp to ignore commit
        timestamps greater than the specified value until the next commit moves
        the tracked commit timestamp forwards.  This is only intended for use
        where the application is rolling back locally committed transactions.
        See @ref transaction_timestamps'''),
    Config('oldest_timestamp', '', r'''
        future commits and queries will be no earlier than the specified
        timestamp. Supplied values must be monotonically increasing.
        See @ref transaction_timestamps'''),
    Config('stable_timestamp', '', r'''
        checkpoints will not include commits that are newer than the specified
        timestamp in tables configured with \c log=(enabled=false).  Supplied
        values must be monotonically increasing.  The stable timestamp data
        stability only applies to tables that are not being logged.  See @ref
        transaction_timestamps'''),
]),

'WT_CONNECTION.rollback_to_stable' : Method([]),

'WT_SESSION.reconfigure' : Method(session_config),

# There are 4 variants of the wiredtiger_open configurations.
# wiredtiger_open:
#    Configuration values allowed in the application's configuration
#    argument to the wiredtiger_open call.
# wiredtiger_open_basecfg:
#    Configuration values allowed in the WiredTiger.basecfg file (remove
# creation-specific configuration strings and add a version string).
# wiredtiger_open_usercfg:
#    Configuration values allowed in the WiredTiger.config file (remove
# creation-specific configuration strings).
# wiredtiger_open_all:
#    All of the above configuration values combined
'wiredtiger_open' : Method(wiredtiger_open),
'wiredtiger_open_basecfg' : Method(wiredtiger_open_common + [
    Config('version', '(major=0,minor=0)', r'''
        the file version'''),
]),
'wiredtiger_open_usercfg' : Method(wiredtiger_open_common),
'wiredtiger_open_all' : Method(wiredtiger_open + [
    Config('version', '(major=0,minor=0)', r'''
        the file version'''),
]),
}
