#define __STDC_LIMIT_MACROS   // needed to get UINT64_MAX in C++
#include <iomanip>
#include <iostream>
#include <sstream>
#include "wiredtiger.h"
#include "workgen.h"
#include "workgen_time.h"
extern "C" {
// Include some specific WT files, as some files included by wt_internal.h
// have some C-ism's that don't work in C++.
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include "error.h"
#include "misc.h"
}

#define MINIMUM_KEY_SIZE  12        // The minimum key must contain
#define MINIMUM_VALUE_SIZE  12

#define MIN(a, b)		((a) < (b) ? (a) : (b))
#define MAX(a, b)		((a) < (b) ? (b) : (a))
#define TIMESPEC_DOUBLE(ts)	((double)(ts).tv_sec + ts.tv_nsec * 0.000000001)
#define PCT(n, total)		((total) == 0 ? 0 : ((n) * 100) / (total))
#define OPS_PER_SEC(ops, secs)	(int) ((secs) == 0 ? 0.0 : \
    (ops) / TIMESPEC_DOUBLE(secs))

#define ASSERT(cond)                                                    \
    do {                                                                \
        if (!(cond)) {                                                  \
            fprintf(stderr, "%s:%d: ASSERT failed: %s\n",               \
                    __FILE__, __LINE__, #cond);                         \
            abort();                                                    \
        }                                                               \
    } while(0)

#define VERBOSE(env, args)                                              \
    do {                                                                \
        if (env._context->_verbose)                                     \
            std::cout << args << std::endl;                             \
    } while(0)

#define OP_HAS_VALUE(op) \
    ((op)->_optype == OP_INSERT || (op)->_optype == OP_UPDATE)

namespace workgen {

int execute(WT_CONNECTION *wt_conn, Workload &workload) {
    return (workload.run(wt_conn));
}

static void *thread_runner(void *arg) {
    ThreadEnvironment *env = (ThreadEnvironment *)arg;
    env->_errno = env->_thread->run(*env);
    return (NULL);
}

// Exponentiate (like the pow function), except that it returns an exact
// integral 64 bit value, and if it overflows, returns the maximum possible
// value for the return type.
uint64_t power64(int base, int exp) {
    uint64_t last, result;

    result = 1;
    for (int i = 0; i < exp; i++) {
        last = result;
        result *= base;
        if (result < last)
            return UINT64_MAX;
    }
    return result;
}

static void set_kv_max(int size, uint64_t *max) {
    *max = power64(10, (size - 1) - 1);
}


Context::Context() : _verbose(false), _table_count() {
}

Context::~Context() {}

void Key::gen(uint64_t n, char *result) const {
    if (n > _max) {
        std::stringstream sstm;
        sstm << "Key (" << n << ") too large for size (" << _size << ")";
        WorkgenException wge(0, sstm.str().c_str());
	throw(wge);
    }
    workgen_u64_to_string_zf(n, result, _size);
}

void Key::compute_max() {
    if (_size < 2) {
        WorkgenException wge(0, "Key.size too small");
	throw(wge);
    }
    set_kv_max(_size, &_max);
}

void Key::size_buffer(size_t &keysize) const {
    if ((size_t)_size > keysize)
        keysize = _size;
}

void Value::gen(uint64_t n, char *result) const {
    if (n > _max) {
        std::stringstream sstm;
        sstm << "Value (" << n << ") too large for size (" << _size << ")";
        WorkgenException wge(0, sstm.str().c_str());
	throw(wge);
    }
    workgen_u64_to_string_zf(n, result, _size);
}

void Value::compute_max() {
    if (_size < 2) {
        WorkgenException wge(0, "Value.size too small");
	throw(wge);
    }
    set_kv_max(_size, &_max);
}

void Value::size_buffer(size_t &valuesize) const {
    if ((size_t)_size > valuesize)
        valuesize = _size;
}

ThreadEnvironment::ThreadEnvironment() :
    _errno(0), _thread(NULL), _context(NULL), _rand_state(NULL) {
}

ThreadEnvironment::~ThreadEnvironment() {
    if (_rand_state != NULL) {
        workgen_random_free(_rand_state);
        _rand_state = NULL;
    }
}

int ThreadEnvironment::create(WT_SESSION *session) {
    WT_RET(workgen_random_alloc(session, &_rand_state));
    return (0);
}

Thread::Thread() : _ops(), _name(), _stop(false), _count(0), _session(NULL),
    _keybuf(NULL), _valuebuf(NULL), _repeat(false) {
}

Thread::Thread(const std::vector<Operation> &ops, int count) : _ops(ops),
    _name(), _stop(false), _count(count), _session(NULL), _keybuf(NULL),
    _valuebuf(NULL), _repeat(false) {
}

Thread::Thread(const Thread &other) : _ops(other._ops), _name(other._name),
    _stop(false), _count(other._count), _session(NULL), _keybuf(NULL),
    _valuebuf(NULL), _repeat(false) {
    // Note: a partial copy, we only want one thread to own _keybuf, _valuebuf.
}

Thread::~Thread() {
    free_all();
}

void Thread::free_all() {
    if (_keybuf != NULL) {
        delete _keybuf;
        _keybuf = NULL;
    }
    if (_valuebuf != NULL) {
        delete _valuebuf;
        _valuebuf = NULL;
    }
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

int Thread::create_all(WT_CONNECTION *conn, ThreadEnvironment &env) {
    size_t keysize, valuesize;

    WT_RET(close_all());
    ASSERT(_session == NULL);
    WT_RET(conn->open_session(conn, NULL, NULL, &_session));
    WT_RET(env.create(_session));
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
    if (_session != NULL) {
        WT_RET(_session->close(_session, NULL));
        _session = NULL;
    }
    free_all();
    return (0);
}

int Thread::run(ThreadEnvironment &env) {
    WT_DECL_RET;
    std::string name = env._thread->_name;
    VERBOSE(env, "thread " << name << " running");

    for (int cnt = 0; !_stop && (_repeat || cnt < _count); cnt++)
        for (std::vector<Operation>::iterator i = _ops.begin();
          !_stop && i != _ops.end(); i++)
            WT_TRET(i->run(env));

    if (ret != 0)
        std::cerr << "thread " << name << " failed err="
                  << ret << std::endl;
    VERBOSE(env, "thread " << name << "finished");
    return (ret);
}

void Thread::get_stats(TableStats &stats) {
    for (std::vector<Operation>::iterator i = _ops.begin();
      i != _ops.end(); i++)
        i->get_stats(stats);
}

void Thread::get_static_counts(TableStats &stats)
{
    for (std::vector<Operation>::iterator i = _ops.begin();
      i != _ops.end(); i++)
        i->get_static_counts(stats);
}

void Thread::clear_stats() {
    for (std::vector<Operation>::iterator i = _ops.begin();
      i != _ops.end(); i++)
        i->clear_stats();
}

Operation::Operation() :
    _optype(OP_NONE), _table(), _key(), _value(), _children(NULL),
    _repeatchildren(0) {
}

Operation::Operation(OpType optype, Table table, Key key, Value value) :
    _optype(optype), _table(table), _key(key), _value(value), _children(NULL),
    _repeatchildren(0) {
}

Operation::Operation(OpType optype, Table table, Key key) :
    _optype(optype), _table(table), _key(key), _value(), _children(NULL),
    _repeatchildren(0) {
    if (OP_HAS_VALUE(this)) {
        WorkgenException wge(0, "OP_INSERT and OP_UPDATE require a value");
	throw(wge);
    }
}

Operation::Operation(const Operation &other) :
    _optype(other._optype), _table(other._table), _key(other._key),
    _value(other._value), _children(other._children),
    _repeatchildren(other._repeatchildren) {
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
        os << ", children[" << _repeatchildren << "]: {";
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

int Operation::run(ThreadEnvironment &env) {
    WT_CURSOR *cursor = _table._cursor;
    Thread *thread = env._thread;
    uint32_t rand;
    char *buf;

    // TODO: what happens if multiple threads choose the same keys
    // for a table.  Should separate thread-specific key spaces be
    // carved out?
    if (_optype != OP_NONE) {
        uint64_t recno;
        recno = env._context->_table_count[_table._tablename];
        if (_optype != OP_INSERT) {
            rand = workgen_random(env._rand_state);
            if (recno == 0)
                return (WT_NOTFOUND); // TODO
            recno = rand % recno;
        }
        buf = thread->_keybuf;
        _key.gen(recno, buf);
        cursor->set_key(cursor, buf);
        if (OP_HAS_VALUE(this)) {
            buf = thread->_valuebuf;
            _value.gen(recno, buf);
            cursor->set_value(cursor, buf);
        }
        switch (_optype) {
        case OP_INSERT:
            WT_RET(cursor->insert(cursor));
            env._context->_table_count[_table._tablename] = recno + 1;
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
            WT_RET(cursor->update(cursor));
            _table.stats.updates++;
            break;
        default:
            ASSERT(false);
        }
    }
    if (_children != NULL)
        for (int count = 0; !thread->_stop && count < _repeatchildren; count++)
            for (std::vector<Operation>::iterator i = _children->begin();
              i != _children->end(); i++)
                WT_RET(i->run(env));
    return (0);
}

void Operation::get_stats(TableStats &stats) {
    stats.add(_table.stats);
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin();
          i != _children->end(); i++)
            i->get_stats(stats);
}

void Operation::get_static_counts(TableStats &stats)
{
    switch (_optype) {
    case OP_NONE:
        break;
    case OP_INSERT:
        stats.inserts++;
        break;
    case OP_REMOVE:
        stats.removes++;
        break;
    case OP_SEARCH:
        stats.reads++;
        break;
    case OP_UPDATE:
        stats.updates++;
        break;
    default:
        ASSERT(false);
    }
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin();
          i != _children->end(); i++)
            i->get_static_counts(stats);
}

void Operation::clear_stats() {
    _table.stats.clear();
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin();
          i != _children->end(); i++)
            i->clear_stats();
}

int Operation::open_all(WT_SESSION *session) {
    if (_optype != OP_NONE) {
        WT_RET(_table.open_all(session));
        _key.compute_max();
        if (OP_HAS_VALUE(this))
            _value.compute_max();
    }
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

void TableStats::final_report(std::ostream &os, timespec &totalsecs) const {
    uint64_t ops = 0;
    ops += reads;
    ops += inserts;
    ops += updates;
    ops += truncates;
    ops += removes;

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

Table::Table() : _tablename(), stats(), _cursor(NULL) {}
Table::Table(const char *tablename) : _tablename(tablename), stats(),
    _cursor(NULL) {}
Table::Table(const Table &other) : _tablename(other._tablename),
    stats(other.stats), _cursor(NULL) {}
Table::~Table() {}

void Table::describe(std::ostream &os) const {
    os << "Table: " << _tablename;
}

int Table::open_all(WT_SESSION *session) {
    // Tables may be shared within a thread.
    if (_cursor == NULL) {
        WT_RET(session->open_cursor(session, _tablename.c_str(), NULL, NULL,
          &_cursor));
    }
    return (0);
}

Workload::Workload(Context *context, const std::vector<Thread> &threads) :
    _context(context), _threads(threads),
    _run_time(0), _report_interval(0) {
    if (context == NULL) {
        WorkgenException wge(0, "Workload contructor requires a Context");
	throw(wge);
    }
}

Workload::Workload(const Workload &other) :
    _context(other._context), _threads(other._threads), _run_time(0),
    _report_interval(0) {
}

Workload::~Workload() {
}

int Workload::open_all(WT_CONNECTION *conn,
                        std::vector<ThreadEnvironment> &envs) {
    (void)envs;
    for (size_t i = 0; i < _threads.size(); i++) {
        WT_RET(_threads[i].open_all(conn));
    }
    return (0);
}

int Workload::create_all(WT_CONNECTION *conn, Context *context,
                        std::vector<ThreadEnvironment> &envs) {
    for (size_t i = 0; i < _threads.size(); i++) {
        std::stringstream sstm;
        sstm << "thread" << i;
        _threads[i]._name = sstm.str();
        envs[i]._thread = &_threads[i];
        envs[i]._context = context;
        // TODO: recover from partial failure here
        WT_RET(_threads[i].create_all(conn, envs[i]));
    }
    return (0);
}

int Workload::close_all() {
    for (size_t i = 0; i < _threads.size(); i++)
        _threads[i].close_all();

    return (0);
}

int Workload::run(WT_CONNECTION *conn) {
    WT_DECL_RET;
    std::vector<ThreadEnvironment> envs(_threads.size());

    WT_ERR(create_all(conn, _context, envs));
    WT_ERR(open_all(conn, envs));
    WT_ERR(run_all(envs));

  err:
    //TODO: (void)close_all();
    return (ret);
}

void Workload::get_stats(TableStats &stats) {
    for (size_t i = 0; i < _threads.size(); i++)
        _threads[i].get_stats(stats);
}

void Workload::clear_stats() {
    for (size_t i = 0; i < _threads.size(); i++)
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

void Workload::final_report(timespec &totalsecs) {
    TableStats stats;

    get_stats(stats);
    stats.final_report(std::cout, totalsecs);
    std::cout << "Run completed: " << totalsecs << " seconds" << std::endl;
}

int Workload::run_all(std::vector<ThreadEnvironment> &envs) {
    void *status;
    std::vector<pthread_t> thread_handles;
    TableStats stats;
    WT_DECL_RET;

    for (size_t i = 0; i < _threads.size(); i++)
        _threads[i].get_static_counts(stats);
    std::cout << "Starting workload: " << _threads.size() << " threads, ";
    stats.report(std::cout);
    std::cout << std::endl;

    for (size_t i = 0; i < _threads.size(); i++) {
        pthread_t thandle;
        _threads[i].clear_stats();
        _threads[i]._stop = false;
        _threads[i]._repeat = (_run_time != 0);
        if ((ret = pthread_create(&thandle, NULL, thread_runner,
          &envs[i])) != 0) {
            std::cerr << "pthread_create failed err=" << ret << std::endl;
            std::cerr << "Stopping all threads." << std::endl;
            for (size_t j = 0; j < thread_handles.size(); j++) {
                _threads[j]._stop = true;
                (void)pthread_join(thread_handles[j], &status);
                _threads[j].close_all();
            }
            return (ret);
        }
        thread_handles.push_back(thandle);
    }

    timespec start;
    workgen_epoch(&start);
    timespec end = start + _run_time;
    timespec next_report = start + _report_interval;

    stats.clear();
    timespec now = start;
    while (now < end) {
        timespec sleep_amt;

        sleep_amt = end - now;
        if (next_report != 0) {
            timespec next_diff = next_report - now;
            if (next_diff < next_report)
                sleep_amt = next_diff;
        }
        if (sleep_amt.tv_sec > 0)
            sleep(sleep_amt.tv_sec);
        else
            usleep((sleep_amt.tv_nsec + 999)/ 1000);

        workgen_epoch(&now);
        if (now >= next_report && now < end && _report_interval != 0) {
            report(_report_interval, (now - start).tv_sec, stats);
            while (now >= next_report)
                next_report += _report_interval;
        }
    }
    if (_run_time != 0)
        for (size_t i = 0; i < _threads.size(); i++)
            _threads[i]._stop = true;

    for (size_t i = 0; i < _threads.size(); i++) {
        WT_TRET(pthread_join(thread_handles[i], &status));
        if (envs[i]._errno != 0)
            VERBOSE(envs[i],
                    "Thread " << i << " has errno " << envs[i]._errno);
        WT_TRET(envs[i]._errno);
        _threads[i].close_all();
    }
    timespec finalsecs = now - start;
    final_report(finalsecs);

    if (ret != 0)
        std::cerr << "run_all failed err=" << ret << std::endl;
    std::cout << std::endl;
    return (ret);
}

};
