#include <iostream>
#include <sstream>
#include "wiredtiger.h"
#include "workgen.h"
//#include "test_util.h"   // TODO: cannot use yet, it includes wt_internal.h
extern "C" {
// Include specific files, as some files included by wt_internal.h
// have some C-ism's that don't work in C++.
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "error.h"
#include "misc.h"
}

#define MINIMUM_KEY_SIZE  12        // The minimum key must contain
#define MINIMUM_VALUE_SIZE  12

#define MIN(a, b)	((a) < (b) ? (a) : (b))
#define MAX(a, b)	((a) < (b) ? (b) : (a))
#define ASSERT(cond)                                                    \
    do {                                                                \
        if (!(cond)) {                                                  \
            fprintf(stderr, "%s:%d: ASSERT failed: %s\n",               \
                    __FILE__, __LINE__, #cond);                         \
            abort();                                                    \
        }                                                               \
    } while(0)

#define VERBOSE(context, args)                                          \
    do {                                                                \
        if (context._verbose)                                           \
            std::cout << args << std::endl;                             \
    } while(0)

namespace workgen {

int execute(WT_CONNECTION *wt_conn, Workload &workload) {
    return (workload.run(wt_conn));
}

static void *thread_runner(void *arg) {
    WorkgenContext *context = (WorkgenContext *)arg;
    context->_errno = context->_thread->run(*context);
    return (NULL);
}

void Key::gen(uint64_t n, char *result) const {
    // TODO: use u64_to_string_zf(n, result, _size)
    int s = MAX(_size, MINIMUM_KEY_SIZE);
    sprintf(result, "%10.10d", (int)n);
    memset(&result[10], '.', s - 11);
    result[s - 1] = '\0';
}

void Key::size_buffer(size_t &keysize) const {
    if ((size_t)_size > keysize)
        keysize = _size;
}

void Value::gen(uint64_t n, char *result) const {
    // TODO: use u64_to_string_zf(n, result, _size)
    int s = MAX(_size, MINIMUM_VALUE_SIZE);
    sprintf(result, "%10.10d", (int)n);
    memset(&result[10], '.', s - 11);
    result[s - 1] = '\0';
}

void Value::size_buffer(size_t &valuesize) const {
    if ((size_t)_size > valuesize)
        valuesize = _size;
}

Thread::Thread() : _ops(), _name(), _stop(false), _session(NULL), _keybuf(NULL),
    _valuebuf(NULL) {
}

Thread::Thread(const std::vector<Operation> &ops) : _ops(ops), _name(),
    _stop(false), _session(NULL), _keybuf(NULL), _valuebuf(NULL) {
}

Thread::Thread(const Thread &other) : _ops(other._ops), _name(other._name),
    _stop(false), _session(NULL), _keybuf(NULL), _valuebuf(NULL) {
}

Thread::~Thread() {
    if (_keybuf != NULL)
        delete _keybuf;
    if (_valuebuf != NULL)
        delete _valuebuf;
}

void Thread::describe(std::ostream &os) const {
    os << "Thread: [" << std::endl;
    for (std::vector<Operation>::const_iterator i = _ops.begin(); i != _ops.end(); i++) {
        os << "  "; i->describe(os); os << std::endl;
    }
    os << "]";
}

int Thread::open_all(WT_CONNECTION *conn) {
    (void)conn;
    for (std::vector<Operation>::iterator i = _ops.begin(); i != _ops.end();
         i++) {
        WT_RET(i->open_all(_session));
    }
    return (0);
}

int Thread::create_all(WT_CONNECTION *conn) {
    size_t keysize, valuesize;

    ASSERT(_session == NULL);
    WT_RET(conn->open_session(conn, NULL, NULL, &_session));
    keysize = MINIMUM_KEY_SIZE;
    valuesize = MINIMUM_VALUE_SIZE;
    for (std::vector<Operation>::iterator i = _ops.begin(); i != _ops.end();
         i++)
        i->size_buffers(keysize, valuesize);
    _keybuf = new char[keysize];
    _valuebuf = new char[valuesize];
    _keybuf[keysize - 1] = '\0';
    _valuebuf[valuesize - 1] = '\0';
    return (0);
}

int Thread::close_all() {
    if (_session != NULL)
        WT_RET(_session->close(_session, NULL));
    return (0);
}

int Thread::run(WorkgenContext &context) {
    WT_DECL_RET;
    std::string name = context._thread->_name;
    VERBOSE(context, "thread " << name << " running");

    context._nrecords = 0;
    for (std::vector<Operation>::iterator i = _ops.begin();
      !_stop && i != _ops.end(); i++) {
        WT_TRET(i->run(context));
        if (ret != 0) {
            std::cerr << "thread " << name << " failed err="
                      << ret << std::endl;
        }
    }
    VERBOSE(context, "thread " << name << "finished");
    return (ret);
}

void Thread::get_stats(TableStats &stats) {
    for (std::vector<Operation>::iterator i = _ops.begin();
      i != _ops.end(); i++)
        i->get_stats(stats);
}

void Thread::clear_stats() {
    for (std::vector<Operation>::iterator i = _ops.begin();
      i != _ops.end(); i++)
        i->clear_stats();
}

Operation::Operation() :
    _optype(OP_NONE), _table(), _key(), _value(), _children(NULL),
    _repeatchildren(0), _repeatinf(false) {
}

Operation::Operation(OpType optype, Table table, Key key, Value value) :
    _optype(optype), _table(table), _key(key), _value(value), _children(NULL),
    _repeatchildren(0), _repeatinf(false) {
}

Operation::Operation(OpType optype, Table table, Key key) :
    _optype(optype), _table(table), _key(key), _value(), _children(NULL),
    _repeatchildren(0), _repeatinf(false) {
    if (_optype == OP_INSERT || _optype == OP_UPDATE) {
        WorkgenException wge(0, "OP_INSERT and OP_UPDATE require a value");
	throw(wge);
    }
}

Operation::Operation(const Operation &other) :
    _optype(other._optype), _table(other._table), _key(other._key),
    _value(other._value), _children(other._children),
    _repeatchildren(other._repeatchildren), _repeatinf(other._repeatinf) {
    // Creation and destruction of _children is managed by Python.
}

Operation::~Operation()
{
    // Creation and destruction of _children is managed by Python.
}

void Operation::describe(std::ostream &os) const {
    os << "Operation: " << _optype;
    if (_optype != OP_NONE) {
        os << ", ";  _table.describe(os);
        os << ", "; _key.describe(os);
        os << ", "; _value.describe(os);
    }
    if (_children != NULL) {
        os << ", children[";
        if (_repeatinf)
            os << "inf";
        else
            os << _repeatchildren;
        os << "]: {";
        bool first = true;
        for (std::vector<Operation>::const_iterator i = _children->begin();
             i != _children->end(); i++) {
            if (!first)
                os << "}, {";
            i->describe(os);
            first = false;
        }
        os << "}";
    }
}

void Operation::size_buffers(size_t &keysize, size_t &valuesize) const
{
    if (_optype != OP_NONE) {
        _key.size_buffer(keysize);
        _value.size_buffer(valuesize);
    }
    if (_children != NULL)
        for (std::vector<Operation>::const_iterator i = _children->begin();
            i != _children->end(); i++)
            i->size_buffers(keysize, valuesize);
}

int Operation::run(WorkgenContext &context) {
    WT_CURSOR *cursor = _table._cursor;
    Thread *thread = context._thread;
    char *buf;

    // TODO: what happens if multiple threads choose the same keys
    // for a table.  Should separate thread-specific key spaces be
    // carved out?
    if (_optype != OP_NONE) {
        uint64_t recno;
        if (_optype == OP_INSERT)
            recno = context._nrecords;
        else
            // TODO: choose a recno based on distribution
            recno = context._nrecords / 2;
        buf = thread->_keybuf;
        _key.gen(recno, buf);
        cursor->set_key(cursor, buf);
        switch (_optype) {
        case OP_INSERT:
            buf = thread->_valuebuf;
            _value.gen(context._nrecords, buf);
            cursor->set_value(cursor, buf);
            WT_RET(cursor->insert(cursor));
            context._nrecords++;
            _table.stats.inserts++;
            break;
        case OP_REMOVE:
            WT_RET(cursor->remove(cursor));
            _table.stats.removes++;
            break;
        case OP_SEARCH:
            WT_RET(cursor->search(cursor));
            _table.stats.reads++;
            break;
        case OP_UPDATE:
            buf = thread->_valuebuf;
            _value.gen(context._nrecords, buf);
            cursor->set_value(cursor, buf);
            WT_RET(cursor->update(cursor));
            _table.stats.updates++;
            break;
        default:
            ASSERT(false);
        }
    }
    if (_children != NULL)
        for (int count = 0;
          !thread->_stop && (_repeatinf || count < _repeatchildren); count++)
            for (std::vector<Operation>::iterator i = _children->begin();
              i != _children->end(); i++)
                WT_RET(i->run(context));
    return (0);
}

void Operation::get_stats(TableStats &stats) {
    stats.add(_table.stats);
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin();
          i != _children->end(); i++)
            i->get_stats(stats);
}

void Operation::clear_stats() {
    _table.stats.clear();
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin();
          i != _children->end(); i++)
            i->clear_stats();
}

int Operation::open_all(WT_SESSION *session) {
    if (_optype != OP_NONE)
        WT_RET(_table.open_all(session));
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin();
          i != _children->end(); i++)
            WT_RET(i->open_all(session));
    return (0);
}

void TableStats::add(const TableStats &other) {
    inserts += other.inserts;
    reads += other.reads;
    removes += other.removes;
    updates += other.updates;
    truncates += other.truncates;
}

void TableStats::subtract(const TableStats &other) {
    inserts -= other.inserts;
    reads -= other.reads;
    removes -= other.removes;
    updates -= other.updates;
    truncates -= other.truncates;
}

void TableStats::clear() {
    inserts = 0;
    reads = 0;
    removes = 0;
    updates = 0;
    truncates = 0;
}

void TableStats::report(std::ostream &os) const {
    os << reads << " reads, ";
    os << inserts << " inserts, ";
    os << updates << " updates, ";
    os << truncates << " truncates, ";
    os << removes << " removes";
}

void TableStats::final_report(std::ostream &os, int totalsecs) const {
    uint64_t ops = 0;
    ops += reads;
    ops += inserts;
    ops += updates;
    ops += truncates;
    ops += removes;

#define PCT(n, total)		((total) == 0 ? 0 : ((n) * 100) / (total))
#define OPS_PER_SEC(ops, secs)	((secs) == 0 ? 0 : (ops) / (secs))
#define FINAL_OUTPUT(os, field, singular, ops, totalsecs)               \
    os << "Executed " << field << " " #singular " operations ("         \
       << PCT(field, ops) << "%) " << OPS_PER_SEC(field, totalsecs)     \
       << " ops/sec" << std::endl

    FINAL_OUTPUT(os, reads, read, ops, totalsecs);
    FINAL_OUTPUT(os, inserts, insert, ops, totalsecs);
    FINAL_OUTPUT(os, updates, update, ops, totalsecs);
    FINAL_OUTPUT(os, truncates, truncate, ops, totalsecs);
    FINAL_OUTPUT(os, removes, remove, ops, totalsecs);
}

void TableStats::describe(std::ostream &os) const {
    os << "TableStats: reads " << reads;
    os << ", inserts " << inserts;
    os << ", updates " << updates;
    os << ", truncates " << truncates;
    os << ", removes " << removes;
}

int Table::open_all(WT_SESSION *session) {
    // Tables may be shared within a thread.
    if (_cursor == NULL) {
        WT_RET(session->open_cursor(session, _tablename.c_str(), NULL, NULL,
          &_cursor));
    }
    return (0);
}

int Workload::open_all(WT_CONNECTION *conn,
                        std::vector<WorkgenContext> &contexts) {
    (void)contexts;
    for (int i = 0; i < (int)_threads.size(); i++) {
        WT_RET(_threads[i].open_all(conn));
    }
    return (0);
}

int Workload::create_all(WT_CONNECTION *conn,
                        std::vector<WorkgenContext> &contexts) {
    for (int i = 0; i < (int)_threads.size(); i++) {
        std::stringstream sstm;
        sstm << "thread" << i;
        _threads[i]._name = sstm.str();
        contexts[i]._thread = &_threads[i];
        // TODO: recover from partial failure here
        WT_RET(_threads[i].create_all(conn));
    }
    return (0);
}

int Workload::close_all() {
    for (int i = 0; i < (int)_threads.size(); i++)
        _threads[i].close_all();

    return (0);
}

int Workload::run(WT_CONNECTION *conn) {
    WT_DECL_RET;
    std::vector<WorkgenContext> contexts(_threads.size());

    WT_ERR(create_all(conn, contexts));
    WT_ERR(open_all(conn, contexts));
    WT_ERR(run_all(contexts));

  err:
    //TODO: (void)close_all();
    return (ret);
}

void Workload::get_stats(TableStats &stats) {
    for (int i = 0; i < (int)_threads.size(); i++)
        _threads[i].get_stats(stats);
}

void Workload::clear_stats() {
    for (int i = 0; i < (int)_threads.size(); i++)
        _threads[i].clear_stats();
}

void Workload::report(int interval, int totalsecs, TableStats &totals) {
    TableStats stats;
    get_stats(stats);
    TableStats diff(stats);
    diff.subtract(totals);
    totals = stats;
    diff.report(std::cout);
    std::cout << " in " << interval << " secs ("
              << totalsecs << " total secs)" << std::endl;
}

void Workload::final_report(int totalsecs) {
    TableStats stats;

    get_stats(stats);
    stats.final_report(std::cout, totalsecs);
    std::cout << "Run completed: " << totalsecs << " seconds" << std::endl;
}

int Workload::run_all(std::vector<WorkgenContext> &contexts) {
    void *status;
    std::vector<pthread_t> thread_handles(_threads.size());
    TableStats stats;
    WT_DECL_RET;

    for (int i = 0; i < (int)_threads.size(); i++) {
        _threads[i].clear_stats();
        _threads[i]._stop = false;
        // TODO: on error, clean up already started threads, set _stop flag.
        WT_RET(pthread_create(&thread_handles[i], NULL, thread_runner,
            &contexts[i]));
    }

    time_t start = time(NULL);
    time_t end = start + _run_time;
    time_t next_report = start + _report_interval;

    stats.clear();
    time_t now = start;
    while (now < end) {
        if (next_report == 0)
            sleep(end - now);
        else
            sleep(MIN(end - now, next_report - now));
        now = time(NULL);
        if (now >= next_report && now < end && _report_interval != 0) {
            report(_report_interval, now - start, stats);
            while (now >= next_report)
                next_report += _report_interval;
        }
    }
    if (_run_time != 0)
        for (int i = 0; i < (int)_threads.size(); i++)
            _threads[i]._stop = true;

    for (int i = 0; i < (int)_threads.size(); i++) {
        WT_TRET(pthread_join(thread_handles[i], &status));
        if (contexts[i]._errno != 0)
            VERBOSE(contexts[i],
                    "Thread " << i << " has errno " << contexts[i]._errno);
        WT_TRET(contexts[i]._errno);
        _threads[i].close_all();
    }
    final_report(now - start);

    if (ret != 0)
        std::cerr << "run_all failed err=" << ret << std::endl;
    return (ret);
}

};
