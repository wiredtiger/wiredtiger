#include <ostream>
#include <string>
#include <vector>

namespace workgen {

struct Thread;

#ifndef SWIG
struct WorkgenContext {
    int _errno;
    Thread *_thread;
    bool _verbose;
    int _nrecords;

    WorkgenContext() : _errno(0), _thread(NULL), _verbose(false),
	_nrecords(0) {}
    WorkgenContext(const WorkgenContext &other) : _errno(other._errno),
	_thread(other._thread), _verbose(other._verbose),
	_nrecords(other._nrecords) {}
    ~WorkgenContext() {}
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

#else
struct WorkGenContext;
#endif

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

/* Tables can be shared among operations within a single Thread */
struct Table {
    std::string _tablename;
    TableStats stats;
#ifndef SWIG
    WT_CURSOR *_cursor;
#endif

    /* XXX select table from range */

    Table() : _tablename(), _cursor(NULL) {}
    Table(const char *tablename) : _tablename(tablename), _cursor(NULL) {}
    Table(const Table &other) : _tablename(other._tablename), _cursor(NULL) {}
    ~Table() {}

    void describe(std::ostream &os) const { os << "Table: " << _tablename; }

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
    int run(WorkgenContext &context);
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
    bool _repeat;
#endif

    /* XXX throttle info, etc. */

    Thread();
    Thread(const std::vector<Operation> &ops, int count = 1);
    Thread(const Thread &other);
    ~Thread();

    void describe(std::ostream &os) const;

#ifndef SWIG
    int create_all(WT_CONNECTION *conn);
    int open_all(WT_CONNECTION *conn);
    int close_all();
    void get_stats(TableStats &stats);
    void clear_stats();
    int run(WorkgenContext &context);
#endif
};

struct Workload {
    std::vector<Thread> _threads;
    int _run_time;
    int _report_interval;

    Workload(const std::vector<Thread> &threads) : _threads(threads),
      _run_time(0), _report_interval(0) {}
    Workload(const Workload &other) :
      _threads(other._threads), _run_time(0), _report_interval(0) {}
    ~Workload() {}

    void describe(std::ostream &os) const {
	os << "Workload: run_time " << _run_time;
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
    int create_all(WT_CONNECTION *conn, std::vector<WorkgenContext> &contexts);
    int open_all(WT_CONNECTION *conn, std::vector<WorkgenContext> &contexts);
    int close_all();
    int run_all(std::vector<WorkgenContext> &contexts);
};

int execute(WT_CONNECTION *conn, Workload &workload);
};
