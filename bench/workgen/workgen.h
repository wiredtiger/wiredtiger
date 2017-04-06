#include <ostream>
#include <string>
#include <vector>
#include <map>

namespace workgen {

struct Thread;
struct Context;

#ifndef SWIG
struct ThreadEnvironment {
    int _errno;
    Thread *_thread;
    Context *_context;

    ThreadEnvironment() : _errno(0), _thread(NULL) {}
    ThreadEnvironment(const ThreadEnvironment &other) : _errno(other._errno),
	_thread(other._thread) {}
    ~ThreadEnvironment() {}
};

struct WorkgenException {
    std::string _str;
    WorkgenException() : _str() {}
    WorkgenException(int err, const char *msg = NULL) : _str() {
	if (err != 0)
	    _str += wiredtiger_strerror(err);
	if (msg != NULL) {
	    if (!_str.empty())
		_str += ": ";
	    _str += msg;
	}
    }
    WorkgenException(const WorkgenException &other) : _str(other._str) {}
    ~WorkgenException() {}
};
#endif

struct Context {
    bool _verbose;
#ifndef SWIG
    std::map<std::string, uint64_t> _table_count;
#endif
    Context();
    ~Context();
    void describe(std::ostream &os) const {
	os << "Context: verbose " << (_verbose ? "true" : "false");
    }
};

struct TableStats {
    uint64_t inserts;
    uint64_t reads;
    uint64_t removes;
    uint64_t updates;
    uint64_t truncates;
    TableStats() : inserts(0), reads(0), removes(0), updates(0), truncates(0) {}
    void add(const TableStats&);
    void subtract(const TableStats&);
    void clear();
    void describe(std::ostream &os) const;
    void report(std::ostream &os) const;
    void final_report(std::ostream &os, int totalsecs) const;
};

/* Tables can be shared among operations within a single Thread.
 * TODO: enforce that multiple threads can't share when run() starts
 * and that multiple threads can't share a table??
 * Better: need a version of this that works with multiple threads,
 * for readers just need a cursor per table/thread.  For writers,
 * doling out different portions of the key space?
 */
struct Table {
    std::string _tablename;
    TableStats stats;
#ifndef SWIG
    WT_CURSOR *_cursor;
    int _nentries;              // TODO: needed?
#endif

    /* XXX select table from range */

    Table();
    Table(const char *tablename);
    Table(const Table &other);
    ~Table();

    void describe(std::ostream &os) const;

#ifndef SWIG
    int open_all(WT_SESSION *session);
#endif
};

struct Key {
    typedef enum { KEYGEN_APPEND, KEYGEN_PARETO, KEYGEN_UNIFORM } KeyType;
    KeyType _keytype;
    int _size;

    /* XXX specify more about key distribution */
    Key() : _keytype(KEYGEN_APPEND), _size(0) {}
    Key(KeyType keytype, int size) : _keytype(keytype), _size(size) {}
    Key(const Key &other) : _keytype(other._keytype), _size(other._size) {}
    ~Key() {}

    void describe(std::ostream &os) const { os << "Key: type " << _keytype << ", size " << _size; }

#ifndef SWIG
    void gen(uint64_t, char *) const;
    void size_buffer(size_t &keysize) const;
#endif
};

struct Value {
    int _size;

    /* XXX specify how value is calculated */
    Value() {}
    Value(int size) : _size(size) {}
    Value(const Value &other) : _size(other._size) {}
    ~Value() {}

    void describe(std::ostream &os) const { os << "Value: size " << _size; }

#ifndef SWIG
    void gen(uint64_t, char *) const;
    void size_buffer(size_t &keysize) const;
#endif
};

struct Operation {
    typedef enum { OP_NONE, OP_INSERT, OP_REMOVE, OP_SEARCH, OP_UPDATE } OpType;
    OpType _optype;

    Table _table;
    Key _key;
    Value _value;
    std::vector<Operation> *_children;
    int _repeatchildren;

    Operation();
    Operation(OpType optype, Table table, Key key, Value value);
    Operation(OpType optype, Table table, Key key);
    Operation(const Operation &other);
    ~Operation();

    void describe(std::ostream &os) const;

#ifndef SWIG
    int open_all(WT_SESSION *session);
    int run(ThreadEnvironment &env);
    void size_buffers(size_t &keysize, size_t &valuesize) const;
    void get_stats(TableStats &);
    void clear_stats();
#endif
};

struct Thread {
    std::vector<Operation> _ops;
    std::string _name;
    bool _stop;
    int _count;
#ifndef SWIG
    WT_SESSION *_session;
    char *_keybuf;
    char *_valuebuf;
    void **_rand_state;
    bool _repeat;
#endif

    /* XXX throttle info, etc. */

    Thread();
    Thread(const std::vector<Operation> &ops, int count = 1);
    Thread(const Thread &other);
    ~Thread();

    void describe(std::ostream &os) const;

#ifndef SWIG
    void free_all();
    int create_all(WT_CONNECTION *conn);
    int open_all(WT_CONNECTION *conn);
    int close_all();
    void get_stats(TableStats &stats);
    void clear_stats();
    int run(ThreadEnvironment &env);
#endif
};

struct Workload {
    Context *_context;
    std::vector<Thread> _threads;
    int _run_time;
    int _report_interval;

    Workload(Context *context, const std::vector<Thread> &threads);
    Workload(const Workload &other);
    ~Workload();

    void describe(std::ostream &os) const {
	os << "Workload: ";
	_context->describe(os);
	os << ", run_time " << _run_time;
	os << ", report_interval " << _report_interval;
	os << ", [" << std::endl;
	for (std::vector<Thread>::const_iterator i = _threads.begin(); i != _threads.end(); i++) {
	    os << "  "; i->describe(os); os << std::endl;
	}
	os << "]";
    }
    int run(WT_CONNECTION *conn);
    void report(int, int, TableStats &);
    void final_report(int);

private:
    void get_stats(TableStats &stats);
    void clear_stats();
    int create_all(WT_CONNECTION *conn, Context *context,
	std::vector<ThreadEnvironment> &envs);
    int open_all(WT_CONNECTION *conn, std::vector<ThreadEnvironment> &envs);
    int close_all();
    int run_all(std::vector<ThreadEnvironment> &envs);
};

int execute(WT_CONNECTION *conn, Workload &workload);
};
