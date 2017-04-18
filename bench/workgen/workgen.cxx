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
#include <math.h>
#include "error.h"
#include "misc.h"
}

#define THROTTLE_PER_SEC  20     // times per sec we will throttle

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

Context::Context() : _verbose(false), _tint(), _table_names(),
    _recno(NULL), _recno_alloced(0), _tint_last(0), _context_count(0) {
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
    if (_recno_alloced != _tint_last) {
        // The array references are 1-based, we'll waste one entry.
        uint64_t *new_recno = new uint64_t[_tint_last + 1];
        memcpy(new_recno, _recno, sizeof(uint64_t) * _recno_alloced);
        memset(&new_recno[_recno_alloced], 0,
          sizeof(uint64_t) * (_tint_last - _recno_alloced + 1));
        delete _recno;
        _recno = new_recno;
        _recno_alloced = _tint_last;
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
    _throttle(NULL), _throttle_ops(0), _throttle_limit(0),
    _in_transaction(false), _number(0), _stats(), _table_usage(),
    _cursors(NULL) {
}

ThreadEnvironment::~ThreadEnvironment() {
    free_all();
}

int ThreadEnvironment::create(WT_SESSION *session) {
    WT_RET(workgen_random_alloc(session, &_rand_state));
    _throttle_ops = 0;
    _throttle_limit = 0;
    _in_transaction = 0;
    return (0);
}

int ThreadEnvironment::open(WT_SESSION *session) {
    typedef WT_CURSOR *WT_CURSOR_PTR;
    if (_cursors != NULL)
        delete _cursors;
    _cursors = new WT_CURSOR_PTR[_context->_tint_last + 1];
    memset(_cursors, 0, sizeof (WT_CURSOR *) * (_context->_tint_last + 1));
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
        for (uint32_t i = 0; i < _context->_tint_last; i++)
            if ((cursor = _cursors[i]) != NULL)
                cursor->close(cursor);
    }
    free_all();
    return (0);
}

void ThreadEnvironment::free_all() {
    if (_rand_state != NULL) {
        workgen_random_free(_rand_state);
        _rand_state = NULL;
    }
    if (_cursors != NULL) {
        delete _cursors;
        _cursors = NULL;
    }
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

#ifdef _DEBUG
std::string ThreadEnvironment::get_debug() {
    return (_debug_messages.str());
}
#endif

Throttle::Throttle(ThreadEnvironment &env, double throttle,
    double throttle_burst) : _env(env), _throttle(throttle),
    _burst(throttle_burst), _next_div(), _ops_delta(0), _ops_prev(0),
    _ops_per_div(0), _ms_per_div(0), _started(false) {
    ts_clear(_next_div);
    _ms_per_div = ceill(1000.0 / THROTTLE_PER_SEC);
    _ops_per_div = ceill(_throttle / THROTTLE_PER_SEC);
}

Throttle::~Throttle() {}

// Given a random 32-bit value, return a float value equally distributed
// between -1.0 and 1.0.
float rand_signed(uint32_t r) {
    int sign = ((r & 0x1) == 0 ? 1 : -1);
    return (((float)r * sign) / UINT32_MAX);
}

// Each time throttle is called, we sleep and return a number of operations to
// perform next.  To implement this we keep a time calculation in _next_div set
// initially to the current time + 1/THROTTLE_PER_SEC.  Each call to throttle
// advances _next_div by 1/THROTTLE_PER_SEC, and if _next_div is in the future,
// we sleep for the difference between the _next_div and the current_time.  We
// always return (Thread.options.throttle / THROTTLE_PER_SEC) as the number of
// operations.
//
// The only variation is that the amount of individual sleeps is modified by a
// random amount (which varies more widely as Thread.options.throttle_burst is
// greater).  This has the effect of randomizing how much clumping happens, and
// ensures that multiple threads aren't executing in lock step.
//
int Throttle::throttle(uint64_t op_count, uint64_t *op_limit) {
    uint64_t ops;
    int64_t sleep_ms;
    timespec now;

    workgen_epoch(&now);
    DEBUG_CAPTURE(_env, "throttle: ops=" << op_count);
    if (!_started) {
        _next_div = ts_add_ms(now, _ms_per_div);
        _started = true;
    } else {
        _ops_delta += (op_count - _ops_prev);
        if (now < _next_div) {
            sleep_ms = ts_ms(_next_div - now);
            sleep_ms += (_ms_per_div * _burst *
              rand_signed(workgen_random(_env._rand_state)));
            if (sleep_ms > 0) {
                DEBUG_CAPTURE(_env, ", sleep=" << sleep_ms);
                usleep((useconds_t)(sleep_ms * 1000));
            }
        }
        _next_div = ts_add_ms(_next_div, _ms_per_div);
    }
    ops = _ops_per_div;
    if (_ops_delta < (int64_t)ops) {
        ops -= _ops_delta;
        _ops_delta = 0;
    } else {
        _ops_delta -= ops;
        ops = 0;
    }
    *op_limit = ops;
    _ops_prev = ops;
    DEBUG_CAPTURE(_env, ", return=" << ops << std::endl);
    return (0);
}

ThreadOptions::ThreadOptions() : name(), throttle(0.0), throttle_burst(1.0) {}
ThreadOptions::ThreadOptions(const ThreadOptions &other) :
    name(other.name), throttle(other.throttle),
    throttle_burst(other.throttle_burst) {}
ThreadOptions::~ThreadOptions() {}

Thread::Thread() : options(), _ops(), _stop(false), _count(0),
    _session(NULL), _keybuf(NULL), _valuebuf(NULL), _repeat(false) {
}

Thread::Thread(const std::vector<Operation> &ops, int count) : options(),
    _ops(ops), _stop(false), _count(count), _session(NULL),
    _keybuf(NULL), _valuebuf(NULL), _repeat(false) {
}

Thread::Thread(const Thread &other) : options(other.options), _ops(other._ops),
    _stop(false), _count(other._count), _session(NULL),
    _keybuf(NULL), _valuebuf(NULL), _repeat(false) {
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
    keysize = 1;
    valuesize = 1;
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
    if (env._throttle) {
        delete env._throttle;
        env._throttle = NULL;
    }
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
    std::string name = env._thread->options.name;

    VERBOSE(env, "thread " << name << " running");
    if (options.throttle != 0) {
        env._throttle = new Throttle(env, options.throttle,
          options.throttle_burst);
    }
    for (int cnt = 0; !_stop && (_repeat || cnt < _count) && ret == 0; cnt++)
        for (std::vector<Operation>::iterator i = _ops.begin();
          !_stop && i != _ops.end(); i++)
            WT_ERR(i->run(env));

err:
#ifdef _DEBUG
    {
        std::string messages = env.get_debug();
        if (!messages.empty())
            std::cerr << "DEBUG (thread " << name << "): "
                      << messages << std::endl;
    }
#endif
    if (ret != 0)
        std::cerr << "thread " << name << " failed err="
                  << ret << std::endl;
    VERBOSE(env, "thread " << name << "finished");
    return (ret);
}

void Thread::get_static_counts(TableStats &stats)
{
    for (std::vector<Operation>::iterator i = _ops.begin();
      i != _ops.end(); i++)
        i->get_static_counts(stats);
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
        if (_table._tint == 0) {
            tint_t tint;

            // We are single threaded in this function, so do not have
            // to worry about locking.
            if (env._context->_tint.count(_table._uri) == 0) {
                tint = workgen_atomic_add32(&env._context->_tint_last, 1);
                env._context->_tint[_table._uri] = tint;
                env._context->_table_names[tint] = _table._uri;
            } else
                tint = env._context->_tint[_table._uri];
            _table._tint = tint;
        }
        uint32_t usage_flags = CONTAINER_VALUE(env._table_usage,
          _table._tint, 0);
        if (_optype == OP_SEARCH)
            usage_flags |= ThreadEnvironment::USAGE_READ;
        else
            usage_flags |= ThreadEnvironment::USAGE_WRITE;
        env._table_usage[_table._tint] = usage_flags;
    }
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
            i != _group->end(); i++)
            i->create_all(env, keysize, valuesize);
}

int Operation::run(ThreadEnvironment &env) {
    TableStats *stats;
    Thread *thread = env._thread;
    tint_t tint = _table._tint;
    WT_CURSOR *cursor = env._cursors[tint];
    WT_SESSION *session = thread->_session;
    WT_DECL_RET;
    uint64_t recno, recno_count;
    uint32_t rand;
    char *buf;

    recno = 0;
    stats = &env._stats;
    if (env._throttle != NULL) {
        if (env._throttle_ops >= env._throttle_limit && !env._in_transaction) {
            WT_ERR(env._throttle->throttle(env._throttle_ops,
              &env._throttle_limit));
            env._throttle_ops = 0;
        }
        if (_optype != OP_NONE)
            ++env._throttle_ops;
    }

    // A potential race: thread1 is inserting, and increments
    // Context->_recno[] for fileX.wt. thread2 is doing one of
    // remove/search/update and grabs the new value of Context->_recno[]
    // for fileX.wt.  thread2 randomly chooses the highest recno (which
    // has not yet been inserted by thread1), and when it accesses
    // the record will get WT_NOTFOUND.  It should be somewhat rare
    // (and most likely when the threads are first beginning).  Any
    // WT_NOTFOUND returns are allowed and get their own statistic bumped.
    if (_optype == OP_INSERT)
        recno = workgen_atomic_add64(&env._context->_recno[tint], 1);
    else if (_optype != OP_NONE) {
        recno_count = env._context->_recno[tint];
        if (recno_count == 0)
            // The file has no entries, force a WT_NOTFOUND return.
            recno = 0;
        else {
            rand = workgen_random(env._rand_state);
            recno = rand % recno_count + 1;  // recnos are one-based.
        }
    }
    if (_transaction != NULL) {
        if (env._in_transaction) {
            WorkgenException wge(0, "nested transactions not supported");
            throw(wge);
        }
        session->begin_transaction(session,
          _transaction->_begin_config.c_str());
        env._in_transaction = true;
    }
    if (_optype != OP_NONE) {
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
            stats->inserts++;
            break;
        case OP_REMOVE:
            WT_ERR_NOTFOUND_OK(cursor->remove(cursor));
            if (ret == 0)
                stats->removes++;
            else
                stats->not_found++;
            break;
        case OP_SEARCH:
            ret = cursor->search(cursor);
            if (ret == 0)
                stats->reads++;
            else
                stats->not_found++;
            break;
        case OP_UPDATE:
            WT_ERR_NOTFOUND_OK(cursor->update(cursor));
            if (ret == 0)
                stats->updates++;
            else
                stats->not_found++;
            break;
        default:
            ASSERT(false);
        }
        ret = 0;  // WT_NOTFOUND allowed.
        cursor->reset(cursor);
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
        env._in_transaction = false;
    }
    return (ret);
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
    not_found += other.not_found;
    reads += other.reads;
    removes += other.removes;
    updates += other.updates;
    truncates += other.truncates;
}

void TableStats::subtract(const TableStats &other) {
    inserts -= other.inserts;
    not_found -= other.not_found;
    reads -= other.reads;
    removes -= other.removes;
    updates -= other.updates;
    truncates -= other.truncates;
}

void TableStats::clear() {
    inserts = 0;
    not_found = 0;
    reads = 0;
    removes = 0;
    updates = 0;
    truncates = 0;
}

void TableStats::report(std::ostream &os) const {
    os << reads << " reads";
    if (not_found > 0) {
        os << " (" << not_found << " not found)";
    }
    os << ", " << inserts << " inserts, ";
    os << updates << " updates, ";
    os << truncates << " truncates, ";
    os << removes << " removes";
}

void TableStats::final_report(std::ostream &os, timespec &totalsecs) const {
    uint64_t ops = 0;
    ops += reads;
    ops += not_found;
    ops += inserts;
    ops += updates;
    ops += truncates;
    ops += removes;

#define FINAL_OUTPUT(os, field, singular, ops, totalsecs)               \
    os << "Executed " << field << " " #singular " operations ("         \
       << PCT(field, ops) << "%) " << OPS_PER_SEC(field, totalsecs)     \
       << " ops/sec" << std::endl

    FINAL_OUTPUT(os, reads, read, ops, totalsecs);
    FINAL_OUTPUT(os, not_found, not found, ops, totalsecs);
    FINAL_OUTPUT(os, inserts, insert, ops, totalsecs);
    FINAL_OUTPUT(os, updates, update, ops, totalsecs);
    FINAL_OUTPUT(os, truncates, truncate, ops, totalsecs);
    FINAL_OUTPUT(os, removes, remove, ops, totalsecs);
}

void TableStats::describe(std::ostream &os) const {
    os << "TableStats: reads " << reads;
    if (not_found > 0) {
        os << " (" << not_found << " not found)";
    }
    os << ", inserts " << inserts;
    os << ", updates " << updates;
    os << ", truncates " << truncates;
    os << ", removes " << removes;
}

Table::Table() : _uri(), _tint(0), _context_count(0) {}
Table::Table(const char *uri) : _uri(uri), _tint(0), _context_count(0) {}
Table::Table(const Table &other) : _uri(other._uri), _tint(0),
    _context_count(0) {}
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
        if (_threads[i].options.name.empty()) {
            sstm << "thread" << i;
            _threads[i].options.name = sstm.str();
        }
        envs[i]._thread = &_threads[i];
        envs[i]._context = context;
        envs[i]._number = (uint32_t)i;
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

void Workload::get_stats(std::vector<ThreadEnvironment> &envs,
  TableStats &stats) {
    for (size_t i = 0; i < _threads.size(); i++)
        stats.add(envs[i]._stats);
}

void Workload::report(std::vector<ThreadEnvironment> &envs, time_t interval,
  time_t totalsecs, TableStats &totals) {
    TableStats stats;
    get_stats(envs, stats);
    TableStats diff(stats);
    diff.subtract(totals);
    totals = stats;
    diff.report(std::cout);
    std::cout << " in " << interval << " secs ("
              << totalsecs << " total secs)" << std::endl;
}

void Workload::final_report(std::vector<ThreadEnvironment> &envs,
  timespec &totalsecs) {
    TableStats stats;

    get_stats(envs, stats);
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
        envs[i]._stats.clear();
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
            sleep((unsigned int)sleep_amt.tv_sec);
        else
            usleep((useconds_t)((sleep_amt.tv_nsec + 999)/ 1000));

        workgen_epoch(&now);
        if (now >= next_report && now < end && options.report_interval != 0) {
            report(envs, options.report_interval, (now - start).tv_sec, stats);
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
    final_report(envs, finalsecs);

    if (ret != 0)
        std::cerr << "run_all failed err=" << ret << std::endl;
    std::cout << std::endl;
    return (ret);
}

};
