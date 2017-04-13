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

// Get the value of a STL container, even if it is not present
#define CONTAINER_VALUE(container, idx, dfault)   \
    (((container).count(idx) > 0) ? (container)[idx] : (dfault))

#define CROSS_USAGE(a, b)                                               \
    (((a & USAGE_READ) != 0 && (b & USAGE_WRITE) != 0) ||               \
     ((a & USAGE_WRITE) != 0 && (b & USAGE_READ) != 0))

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

// The number of contexts.  Normally there is one context created, but it will
// be possible to use several eventually.  More than one is not yet
// implemented, but we must at least guard against the caller creating more
// than one.
static uint32_t context_count = 0;

static void *thread_runner(void *arg) {
    ThreadEnvironment *env = (ThreadEnvironment *)arg;
    env->_errno = env->_thread->run(*env);
    return (NULL);
}

// Exponentiate (like the pow function), except that it returns an exact
// integral 64 bit value, and if it overflows, returns the maximum possible
// value for the return type.
static uint64_t power64(int base, int exp) {
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

Context::Context() : _verbose(false), _recno_index(), _table_names(),
    _recno(NULL), _recno_length(0), _recno_next(0), _context_count(0) {
    uint32_t count;
    if ((count = workgen_atomic_add32(&context_count, 1)) != 1) {
        WorkgenException wge(0, "multiple Contexts not supported");
	throw(wge);
    }
    _context_count = count;
}

Context::~Context() {
    if (_recno != NULL)
        delete _recno;
}

int Context::create_all() {
    if (_recno_length != _recno_next) {
        // The array references are 1-based, we'll waste one entry.
        uint64_t *new_recno = new uint64_t[_recno_next + 1];
        memcpy(new_recno, _recno, sizeof(uint64_t) * _recno_length);
        memset(&new_recno[_recno_length], 0,
          sizeof(uint64_t) * (_recno_next - _recno_length + 1));
        delete _recno;
        _recno = new_recno;
        _recno_length = _recno_next;
    }
    return (0);
}

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
    _errno(0), _thread(NULL), _context(NULL), _rand_state(NULL),
    _table_usage(), _cursors(NULL) {
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

int ThreadEnvironment::open(WT_SESSION *session) {
    typedef WT_CURSOR *WT_CURSOR_PTR;
    if (_cursors != NULL)
        delete _cursors;
    _cursors = new WT_CURSOR_PTR[_context->_recno_next + 1];
    memset(_cursors, 0, sizeof (WT_CURSOR *) * (_context->_recno_next + 1));
    for (std::map<uint32_t, uint32_t>::iterator i = _table_usage.begin();
         i != _table_usage.end(); i++) {
        uint32_t tindex = i->first;
        const char *uri = _context->_table_names[tindex].c_str();
        WT_RET(session->open_cursor(session, uri, NULL, NULL,
          &_cursors[tindex]));
    }
    return (0);
}

int ThreadEnvironment::close() {
    WT_CURSOR *cursor;

    if (_cursors != NULL) {
        for (uint32_t i = 0; i < _context->_recno_next; i++)
            if ((cursor = _cursors[i]) != NULL)
                cursor->close(cursor);
        delete _cursors;
        _cursors = NULL;
    }
    return (0);
}

int ThreadEnvironment::cross_check(std::vector<ThreadEnvironment> &envs) {
    std::map<uint32_t, uint32_t> usage;

    // Determine which tables have cross usage
    for (std::vector<ThreadEnvironment>::iterator e = envs.begin(); e != envs.end(); e++) {
        for (std::map<uint32_t, uint32_t>::iterator i = e->_table_usage.begin();
          i != e->_table_usage.end(); i++) {
            uint32_t tindex = i->first;
            uint32_t thisusage = i->second;
            uint32_t curusage = CONTAINER_VALUE(usage, tindex, 0);
            if (CROSS_USAGE(curusage, thisusage))
                curusage |= USAGE_MIXED;
            usage[tindex] = curusage;
        }
    }
    for (std::map<uint32_t, uint32_t>::iterator i = usage.begin();
         i != usage.end(); i++) {
        if ((i->second & USAGE_MIXED) != 0) {
            for (std::vector<ThreadEnvironment>::iterator e = envs.begin();
                 e != envs.end(); e++) {
                e->_table_usage[i->first] |= USAGE_MIXED;
            }
        }
    }
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

int Thread::open_all(ThreadEnvironment &env) {
    env.open(_session);
    for (std::vector<Operation>::iterator i = _ops.begin(); i != _ops.end();
         i++) {
        WT_RET(i->open_all(_session, env));
    }
    return (0);
}

int Thread::create_all(WT_CONNECTION *conn, ThreadEnvironment &env) {
    size_t keysize, valuesize;

    WT_RET(close_all(env));
    ASSERT(_session == NULL);
    WT_RET(conn->open_session(conn, NULL, NULL, &_session));
    WT_RET(env.create(_session));
    env._table_usage.clear();
    keysize = MINIMUM_KEY_SIZE;
    valuesize = MINIMUM_VALUE_SIZE;
    for (std::vector<Operation>::iterator i = _ops.begin(); i != _ops.end();
         i++)
        i->create_all(env, keysize, valuesize);
    _keybuf = new char[keysize];
    _valuebuf = new char[valuesize];
    _keybuf[keysize - 1] = '\0';
    _valuebuf[valuesize - 1] = '\0';
    return (0);
}

int Thread::close_all(ThreadEnvironment &env) {
    WT_RET(env.close());
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
    _optype(OP_NONE), _table(), _key(), _value(), _transaction(NULL),
    _group(NULL), _repeatgroup(0) {
}

Operation::Operation(OpType optype, Table table, Key key, Value value) :
    _optype(optype), _table(table), _key(key), _value(value),
    _transaction(NULL), _group(NULL), _repeatgroup(0) {
}

Operation::Operation(OpType optype, Table table, Key key) :
    _optype(optype), _table(table), _key(key), _value(), _transaction(NULL),
    _group(NULL), _repeatgroup(0) {
    if (OP_HAS_VALUE(this)) {
        WorkgenException wge(0, "OP_INSERT and OP_UPDATE require a value");
	throw(wge);
    }
}

Operation::Operation(const Operation &other) :
    _optype(other._optype), _table(other._table), _key(other._key),
    _value(other._value), _transaction(other._transaction),
    _group(other._group), _repeatgroup(other._repeatgroup) {
    // Creation and destruction of _group and _transaction is managed
    // by Python.
    // TODO: anything more to do, like add to Python's reference count?
}

Operation::~Operation()
{
    // Creation and destruction of _group, _transaction is managed by Python.
}

void Operation::describe(std::ostream &os) const {
    os << "Operation: " << _optype;
    if (_optype != OP_NONE) {
        os << ", ";  _table.describe(os);
        os << ", "; _key.describe(os);
        os << ", "; _value.describe(os);
    }
    if (_transaction != NULL) {
        os << ", ["; _transaction->describe(os); os << "]";
    }
    if (_group != NULL) {
        os << ", group[" << _repeatgroup << "]: {";
        bool first = true;
        for (std::vector<Operation>::const_iterator i = _group->begin();
             i != _group->end(); i++) {
            if (!first)
                os << "}, {";
            i->describe(os);
            first = false;
        }
        os << "}";
    }
}

void Operation::create_all(ThreadEnvironment &env, size_t &keysize,
  size_t &valuesize) {
    if (_optype != OP_NONE) {
        _key.size_buffer(keysize);
        _value.size_buffer(valuesize);

        // Note: to support multiple contexts we'd need a generation
        // count whenever we execute.
        if (_table._context_count != 0 &&
          _table._context_count != env._context->_context_count) {
            WorkgenException wge(0, "multiple Contexts not supported");
            throw(wge);
        }
        if (_table._recno_index == 0) {
            uint32_t recno_index;

            // We are single threaded in this function, so do not have
            // to worry about locking.
            if (env._context->_recno_index.count(_table._uri) == 0) {
                recno_index = workgen_atomic_add32(
                  &env._context->_recno_next, 1);
                env._context->_recno_index[_table._uri] = recno_index;
                env._context->_table_names[recno_index] = _table._uri;
            } else
                recno_index = env._context->_recno_index[_table._uri];
            _table._recno_index = recno_index;
        }
        uint32_t usage_flags = CONTAINER_VALUE(env._table_usage,
          _table._recno_index, 0);
        if (_optype == OP_SEARCH)
            usage_flags |= ThreadEnvironment::USAGE_READ;
        else
            usage_flags |= ThreadEnvironment::USAGE_WRITE;
        env._table_usage[_table._recno_index] = usage_flags;
    }
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
            i != _group->end(); i++)
            i->create_all(env, keysize, valuesize);
}

int Operation::run(ThreadEnvironment &env) {
    Thread *thread = env._thread;
    uint32_t recno_index = _table._recno_index;
    WT_CURSOR *cursor = env._cursors[recno_index];
    WT_SESSION *session = thread->_session;
    WT_DECL_RET;
    uint32_t rand;
    char *buf;

    if (_transaction != NULL)
        session->begin_transaction(session,
          _transaction->_begin_config.c_str());
    if (_optype != OP_NONE) {
        ASSERT(env._context->_recno_index[_table._uri] == recno_index);
        uint64_t recno;
        if (_optype == OP_INSERT)
            recno = workgen_atomic_add64(&env._context->_recno[recno_index], 1);
        else {
            uint64_t recno_count = env._context->_recno[recno_index];
            if (recno_count == 0)
                return (WT_NOTFOUND); // TODO: make sure caller deals with it
            rand = workgen_random(env._rand_state);
            recno = rand % recno_count + 1;  // recnos are one-based.
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
            WT_ERR(cursor->insert(cursor));
            _table._stats.inserts++;
            break;
        case OP_REMOVE:
            WT_ERR(cursor->remove(cursor));
            _table._stats.removes++;
            break;
        case OP_SEARCH:
            if ((ret = cursor->search(cursor)) == 0)
                _table._stats.reads++;
            else {
                // If a thread writes the file at the time we read it,
                // we may not see the record (the Context._recno field
                // is incremented before the insert happens).  This is
                // not necessarily a hard error.  With more work, we
                // could determine more closely if mixed accesses apply
                // to this file.  With even more work, we could choose
                // keys that are 'known' to have completed writing.
                //TODO:
                //if (ret == WT_NOTFOUND && (env._table_usage[recno_index] &
                //  ThreadEnvironment::USAGE_MIXED) != 0) {
                if (ret == WT_NOTFOUND) {
                    _table._stats.failed_reads++;
                    ret = 0;
                } else
                    WT_ERR(ret);
            }
            break;
        case OP_UPDATE:
            WT_ERR(cursor->update(cursor));
            _table._stats.updates++;
            break;
        default:
            ASSERT(false);
        }
    }
    if (_group != NULL)
        for (int count = 0; !thread->_stop && count < _repeatgroup; count++)
            for (std::vector<Operation>::iterator i = _group->begin();
              i != _group->end(); i++)
                WT_ERR(i->run(env));
err:
    if (_transaction != NULL) {
        if (ret != 0 || _transaction->_rollback)
            WT_TRET(session->rollback_transaction(session, NULL));
        else
            ret = session->commit_transaction(session,
              _transaction->_commit_config.c_str());
    }
    return (ret);
}

void Operation::get_stats(TableStats &stats) {
    stats.add(_table._stats);
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
          i != _group->end(); i++)
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
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
          i != _group->end(); i++)
            i->get_static_counts(stats);
}

void Operation::clear_stats() {
    _table._stats.clear();
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
          i != _group->end(); i++)
            i->clear_stats();
}

int Operation::open_all(WT_SESSION *session, ThreadEnvironment &env) {
    (void)env;
    if (_optype != OP_NONE) {
        _key.compute_max();
        if (OP_HAS_VALUE(this))
            _value.compute_max();
    }
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
          i != _group->end(); i++)
            WT_RET(i->open_all(session, env));
    return (0);
}

void TableStats::add(const TableStats &other) {
    inserts += other.inserts;
    reads += other.reads;
    failed_reads += other.failed_reads;
    removes += other.removes;
    updates += other.updates;
    truncates += other.truncates;
}

void TableStats::subtract(const TableStats &other) {
    inserts -= other.inserts;
    reads -= other.reads;
    failed_reads -= other.failed_reads;
    removes -= other.removes;
    updates -= other.updates;
    truncates -= other.truncates;
}

void TableStats::clear() {
    inserts = 0;
    reads = 0;
    failed_reads = 0;
    removes = 0;
    updates = 0;
    truncates = 0;
}

void TableStats::report(std::ostream &os) const {
    os << reads << " reads";
    if (failed_reads > 0) {
        os << " (" << failed_reads << " failed)";
    }
    os << ", " << inserts << " inserts, ";
    os << updates << " updates, ";
    os << truncates << " truncates, ";
    os << removes << " removes";
}

void TableStats::final_report(std::ostream &os, timespec &totalsecs) const {
    uint64_t ops = 0;
    ops += reads;
    ops += failed_reads;
    ops += inserts;
    ops += updates;
    ops += truncates;
    ops += removes;

#define FINAL_OUTPUT(os, field, singular, ops, totalsecs)               \
    os << "Executed " << field << " " #singular " operations ("         \
       << PCT(field, ops) << "%) " << OPS_PER_SEC(field, totalsecs)     \
       << " ops/sec" << std::endl

    FINAL_OUTPUT(os, reads, read, ops, totalsecs);
    FINAL_OUTPUT(os, failed_reads, failed read, ops, totalsecs);
    FINAL_OUTPUT(os, inserts, insert, ops, totalsecs);
    FINAL_OUTPUT(os, updates, update, ops, totalsecs);
    FINAL_OUTPUT(os, truncates, truncate, ops, totalsecs);
    FINAL_OUTPUT(os, removes, remove, ops, totalsecs);
}

void TableStats::describe(std::ostream &os) const {
    os << "TableStats: reads " << reads;
    if (failed_reads > 0) {
        os << " (" << failed_reads << " failed)";
    }
    os << ", inserts " << inserts;
    os << ", updates " << updates;
    os << ", truncates " << truncates;
    os << ", removes " << removes;
}

Table::Table() : _uri(), _stats(), _recno_index(0),
    _context_count(0) {}
Table::Table(const char *uri) : _uri(uri), _stats(),
     _recno_index(0), _context_count(0) {}
Table::Table(const Table &other) : _uri(other._uri),
    _stats(other._stats), _recno_index(0), _context_count(0) {}
Table::~Table() {}

void Table::describe(std::ostream &os) const {
    os << "Table: " << _uri;
}

WorkloadOptions::WorkloadOptions() : run_time(0), report_interval(0) {}
WorkloadOptions::WorkloadOptions(const WorkloadOptions &other) :
    run_time(other.run_time), report_interval(other.report_interval) {}
WorkloadOptions::~WorkloadOptions() {}

Workload::Workload(Context *context, const std::vector<Thread> &threads) :
    options(), _context(context), _threads(threads) {
    if (context == NULL) {
        WorkgenException wge(0, "Workload contructor requires a Context");
	throw(wge);
    }
}

Workload::Workload(const Workload &other) :
    options(other.options), _context(other._context), _threads(other._threads) {
}

Workload::~Workload() {
}

int Workload::open_all(std::vector<ThreadEnvironment> &envs) {
    for (size_t i = 0; i < _threads.size(); i++) {
        WT_RET(_threads[i].open_all(envs[i]));
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
    WT_RET(context->create_all());
    return (0);
}

int Workload::close_all(std::vector<ThreadEnvironment> &envs) {
    for (size_t i = 0; i < _threads.size(); i++)
        _threads[i].close_all(envs[i]);

    return (0);
}

int Workload::run(WT_CONNECTION *conn) {
    WT_DECL_RET;
    std::vector<ThreadEnvironment> envs(_threads.size());

    WT_ERR(create_all(conn, _context, envs));
    WT_ERR(open_all(envs));
    WT_ERR(ThreadEnvironment::cross_check(envs));
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

void Workload::report(time_t interval, time_t totalsecs, TableStats &totals) {
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
        _threads[i]._repeat = (options.run_time != 0);
        if ((ret = pthread_create(&thandle, NULL, thread_runner,
          &envs[i])) != 0) {
            std::cerr << "pthread_create failed err=" << ret << std::endl;
            std::cerr << "Stopping all threads." << std::endl;
            for (size_t j = 0; j < thread_handles.size(); j++) {
                _threads[j]._stop = true;
                (void)pthread_join(thread_handles[j], &status);
                _threads[j].close_all(envs[j]);
            }
            return (ret);
        }
        thread_handles.push_back(thandle);
    }

    timespec start;
    workgen_epoch(&start);
    timespec end = start + options.run_time;
    timespec next_report = start + options.report_interval;

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
            usleep((useconds_t)((sleep_amt.tv_nsec + 999)/ 1000));

        workgen_epoch(&now);
        if (now >= next_report && now < end && options.report_interval != 0) {
            report(options.report_interval, (now - start).tv_sec, stats);
            while (now >= next_report)
                next_report += options.report_interval;
        }
    }
    if (options.run_time != 0)
        for (size_t i = 0; i < _threads.size(); i++)
            _threads[i]._stop = true;

    for (size_t i = 0; i < _threads.size(); i++) {
        WT_TRET(pthread_join(thread_handles[i], &status));
        if (envs[i]._errno != 0)
            VERBOSE(envs[i],
                    "Thread " << i << " has errno " << envs[i]._errno);
        WT_TRET(envs[i]._errno);
        _threads[i].close_all(envs[i]);
    }
    timespec finalsecs = now - start;
    final_report(finalsecs);

    if (ret != 0)
        std::cerr << "run_all failed err=" << ret << std::endl;
    std::cout << std::endl;
    return (ret);
}

};
