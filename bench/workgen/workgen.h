/*-
 * Public Domain 2014-2017 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#include <ostream>
#include <string>
#include <vector>
#include <map>
#include <set>

namespace workgen {

struct ContextInternal;
struct TableInternal;
struct Thread;
struct Transaction;
struct WorkloadInternal;

struct Track {
    // Threads maintain the total thread operation and total latency they've
    // experienced; the monitor thread periodically copies these values into
    // the last_XXX fields.

    uint64_t ops;                       // Total operations */
    uint64_t latency_ops;               // Total ops sampled for latency
    uint64_t latency;                   // Total latency */

    uint64_t last_latency_ops;          // Last read by monitor thread
    uint64_t last_latency;

    // Minimum/maximum latency, shared with the monitor thread, that is, the
    // monitor thread clears it so it's recalculated again for each period.

    uint32_t min_latency;                // Minimum latency (uS)
    uint32_t max_latency;                // Maximum latency (uS)

    Track(bool latency_tracking = false);
    Track(const Track &other);
    ~Track();

    void add(const Track&);
    void assign(const Track&);
    void clear();
    void incr();
    void incr_with_latency(uint64_t usecs);
    void subtract(const Track&);
    void track_latency(bool);
    bool track_latency() const { return (us != NULL); }

    void _get_us(long *);
    void _get_ms(long *);
    void _get_sec(long *);

private:
    // Latency buckets. From python, accessed via methods us(), ms(), sec()
    uint32_t *us;                        // < 1us ... 1000us
    uint32_t *ms;                        // < 1ms ... 1000ms
    uint32_t *sec;                       // < 1s 2s ... 100s

    Track & operator=(const Track &other);   // use explicit assign method
};

struct Stats {
    Track insert;
    Track not_found;
    Track read;
    Track remove;
    Track update;
    Track truncate;

    Stats(bool latency = false);
    Stats(const Stats &other);
    ~Stats();

    void add(const Stats&);
    void assign(const Stats&);
    void clear();
    void describe(std::ostream &os) const;
#ifndef SWIG
    void final_report(std::ostream &os, timespec &totalsecs) const;
    void report(std::ostream &os) const;
#endif
    void subtract(const Stats&);
    void track_latency(bool);
    bool track_latency() const { return (insert.track_latency()); }

private:
    Stats & operator=(const Stats &other);   // use explicit assign method
};

// A Context tracks the current record number for each uri, used
// for key generation.
//
struct Context {
    bool _verbose;
    ContextInternal *_internal;

    Context();
    ~Context();
    void describe(std::ostream &os) const {
	os << "Context: verbose " << (_verbose ? "true" : "false");
    }

#ifndef SWIG
    Context& operator=(const Context &other);
#endif
};

struct TableOptions {
    /*! default size of the key, unless overridden by Key.size */
    int key_size;
    /*! default size of the value, unless overridden by Value.size */
    int value_size;

    TableOptions();
    TableOptions(const TableOptions &other);
    ~TableOptions();

    void describe(std::ostream &os) const {
	os << "key_size " << key_size;
	os << ", value_size " << value_size;
    }
};

struct Table {
    TableOptions options;
    std::string _uri;
    TableInternal *_internal;

    /* XXX select table from range */

    Table();
    Table(const char *tablename);
    Table(const Table &other);
    ~Table();

    void describe(std::ostream &os) const;

#ifndef SWIG
    Table& operator=(const Table &other);
#endif
};

struct Key {
    typedef enum {
	KEYGEN_AUTO, KEYGEN_APPEND, KEYGEN_PARETO, KEYGEN_UNIFORM } KeyType;
    KeyType _keytype;
    int _size;

    /* XXX specify more about key distribution */
    Key() : _keytype(KEYGEN_AUTO), _size(0) {}
    Key(KeyType keytype, int size) : _keytype(keytype), _size(size) {}
    Key(const Key &other) : _keytype(other._keytype), _size(other._size) {}
    ~Key() {}

    void describe(std::ostream &os) const {
        os << "Key: type " << _keytype << ", size " << _size; }
};

struct Value {
    int _size;

    /* XXX specify how value is calculated */
    Value() : _size(0) {}
    Value(int size) : _size(size) {}
    Value(const Value &other) : _size(other._size) {}
    ~Value() {}

    void describe(std::ostream &os) const { os << "Value: size " << _size; }
};

struct Operation {
    enum OpType {
	OP_NONE, OP_INSERT, OP_REMOVE, OP_SEARCH, OP_UPDATE };
    OpType _optype;

    Table _table;
    Key _key;
    Value _value;
    Transaction *_transaction;
    std::vector<Operation> *_group;
    int _repeatgroup;

#ifndef SWIG
    int _keysize;    // derived from Key._size and Table.options.key_size
    int _valuesize;
    uint64_t _keymax;
    uint64_t _valuemax;
#endif

    Operation();
    Operation(OpType optype, Table table, Key key, Value value);
    Operation(OpType optype, Table table, Key key);
    Operation(OpType optype, Table table);
    Operation(const Operation &other);
    ~Operation();

    void describe(std::ostream &os) const;
#ifndef SWIG
    Operation& operator=(const Operation &other);
    void get_static_counts(Stats &stats, int multiplier);
    void kv_compute_max(bool);
    void kv_gen(bool, uint64_t, char *) const;
    void kv_size_buffer(bool iskey, size_t &size) const;
    void size_check() const;
#endif
};

struct ThreadOptions {
    std::string name;
    double throttle;
    double throttle_burst;  // 0.0 = steady, 1.0 = moderate bursts, etc.

    ThreadOptions();
    ThreadOptions(const ThreadOptions &other);
    ~ThreadOptions();

    void describe(std::ostream &os) const {
	os << "throttle " << throttle;
    }
};

// This is a list of threads, which may be used in the Workload constructor.
// It participates with ThreadList defined on the SWIG/Python side and
// some Python operators added to Thread to allow Threads to be easily
// composed using '+' and multiplied (by integer counts) using '*'.
// Users of the workgen API in Python don't ever need to use
// ThreadListWrapper or ThreadList.
struct ThreadListWrapper {
    std::vector<Thread> _threads;

    ThreadListWrapper() : _threads() {}
    ThreadListWrapper(const ThreadListWrapper &other) :
	_threads(other._threads) {}
    ThreadListWrapper(const std::vector<Thread> &threads) : _threads(threads) {}
    void extend(const ThreadListWrapper &);
    void append(const Thread &);
    void multiply(const int);
};

struct Thread {
    ThreadOptions options;
    Operation _op;

    Thread();
    Thread(const Operation &op);
    Thread(const Thread &other);
    ~Thread();

    void describe(std::ostream &os) const;
};

struct Transaction {
    bool _rollback;
    std::string _begin_config;
    std::string _commit_config;

    Transaction(const char *_config = NULL) : _rollback(false),
       _begin_config(_config == NULL ? "" : _config), _commit_config() {}

    void describe(std::ostream &os) const {
	os << "Transaction: ";
	if (_rollback)
	    os << "(rollback) ";
	os << "begin_config: " << _begin_config;
	if (!_commit_config.empty())
	    os << ", commit_config: " << _commit_config;
    }
};

struct WorkloadOptions {
    /*! output throughput information every interval seconds, 0 to disable */
    int report_interval;
    /*! total workload seconds */
    int run_time;
    /*! performance logging every interval seconds, 0 to disable */
    int sample_interval;
    /*! how often the latency of operations is measured. 1 for every operation,
     * 2 for every second operation, 3 for every third operation etc.
     */
    int sample_rate;

    WorkloadOptions();
    WorkloadOptions(const WorkloadOptions &other);
    ~WorkloadOptions();

    void describe(std::ostream &os) const {
	os << "run_time " << run_time;
	os << ", report_interval " << report_interval;
    }
};

struct Workload {
    WorkloadOptions options;
    Stats stats;
    Context *_context;
    std::vector<Thread> _threads;
    WorkloadInternal *_internal;

    Workload(Context *context, const ThreadListWrapper &threadlist);
    Workload(Context *context, const Thread &thread);
    Workload(const Workload &other);
    ~Workload();

#ifndef SWIG
    Workload& operator=(const Workload &other);
#endif

    void describe(std::ostream &os) const {
	os << "Workload: ";
	_context->describe(os);
	os << ", ";
	options.describe(os);
	os << ", [" << std::endl;
	for (std::vector<Thread>::const_iterator i = _threads.begin(); i != _threads.end(); i++) {
	    os << "  "; i->describe(os); os << std::endl;
	}
	os << "]";
    }
    int run(WT_CONNECTION *conn);
};

};
