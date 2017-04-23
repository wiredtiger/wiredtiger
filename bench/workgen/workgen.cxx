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

#define LATENCY_US_BUCKETS 1000
#define LATENCY_MS_BUCKETS 1000
#define LATENCY_SEC_BUCKETS 100

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

#define THROW_ERRNO(e, args)                                            \
    do {                                                                \
        std::stringstream sstm;                                         \
        sstm << args;                                                   \
        WorkgenException wge(e, sstm.str().c_str());                    \
        throw(wge);                                                     \
    } while(0)

#define THROW(args)   THROW_ERRNO(0, args)

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
    try {
        env->_errno = env->_thread->run(*env);
    } catch (WorkgenException &wge) {
        env->_exception = wge;
    }
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

Context::Context() : _verbose(false), _tint(), _table_names(),
    _recno(NULL), _recno_alloced(0), _tint_last(0), _context_count(0) {
    uint32_t count;
    if ((count = workgen_atomic_add32(&context_count, 1)) != 1)
        THROW("multiple Contexts not supported");
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

ThreadEnvironment::ThreadEnvironment() :
    _errno(0), _exception(), _thread(NULL), _context(NULL), _workload(NULL),
    _rand_state(NULL), _throttle(NULL), _throttle_ops(0), _throttle_limit(0),
    _in_transaction(false), _number(0), _stats(false), _table_usage(),
    _cursors(NULL) {
}

ThreadEnvironment::~ThreadEnvironment() {
    free_all();
}

int ThreadEnvironment::create(WT_SESSION *session) {
    _table_usage.clear();
    _stats.track_latency(_workload->options.sample_interval > 0);
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
                usleep((useconds_t)ms_to_us(sleep_ms));
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

void
ThreadListWrapper::extend(const ThreadListWrapper &other) {
    for (std::vector<Thread>::const_iterator i = other._threads.begin();
         i != other._threads.end(); i++)
        _threads.push_back(*i);
}

void
ThreadListWrapper::append(const Thread &t) {
    _threads.push_back(t);
}

void
ThreadListWrapper::multiply(const int n) {
    if (n == 0) {
        _threads.clear();
    } else {
        std::vector<Thread> copy(_threads);
        for (int cnt = 1; cnt < n; cnt++)
            extend(copy);
    }
}

Thread::Thread() : options(), _op(), _stop(false),
    _session(NULL), _keybuf(NULL), _valuebuf(NULL), _repeat(false) {
}

Thread::Thread(const Operation &op) : options(),
    _op(op), _stop(false), _session(NULL),
    _keybuf(NULL), _valuebuf(NULL), _repeat(false) {
}

Thread::Thread(const Thread &other) : options(other.options), _op(other._op),
    _stop(false), _session(NULL),
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
    _op.describe(os); os << std::endl;
    os << "]";
}

int Thread::open_all(ThreadEnvironment &env) {
    env.open(_session);
    WT_RET(_op.open_all(_session, env));
    return (0);
}

int Thread::create_all(WT_CONNECTION *conn, ThreadEnvironment &env) {
    size_t keysize, valuesize;

    WT_RET(close_all(env));
    ASSERT(_session == NULL);
    WT_RET(conn->open_session(conn, NULL, NULL, &_session));
    WT_RET(env.create(_session));
    keysize = 1;
    valuesize = 1;
    _op.create_all(env, keysize, valuesize);
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
    for (int cnt = 0; !_stop && (_repeat || cnt < 1) && ret == 0; cnt++)
        WT_ERR(_op.run(env));

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

void Thread::get_static_counts(Stats &stats)
{
    _op.get_static_counts(stats, 1);
}

Operation::Operation() :
    _optype(OP_NONE), _table(), _key(), _value(), _transaction(NULL),
    _group(NULL), _repeatgroup(0),
    _keysize(0), _valuesize(0), _keymax(0), _valuemax(0) {
}

Operation::Operation(OpType optype, Table table, Key key, Value value) :
    _optype(optype), _table(table), _key(key), _value(value),
    _transaction(NULL), _group(NULL), _repeatgroup(0),
    _keysize(0), _valuesize(0), _keymax(0), _valuemax(0) {
    size_check();
}

Operation::Operation(OpType optype, Table table, Key key) :
    _optype(optype), _table(table), _key(key), _value(), _transaction(NULL),
    _group(NULL), _repeatgroup(0),
    _keysize(0), _valuesize(0), _keymax(0), _valuemax(0) {
    size_check();
}

Operation::Operation(OpType optype, Table table) :
    _optype(optype), _table(table), _key(), _value(), _transaction(NULL),
    _group(NULL), _repeatgroup(0),
    _keysize(0), _valuesize(0), _keymax(0), _valuemax(0) {
    size_check();
}

Operation::Operation(const Operation &other) :
    _optype(other._optype), _table(other._table), _key(other._key),
    _value(other._value), _transaction(other._transaction),
    _group(other._group), _repeatgroup(other._repeatgroup),
    _keysize(other._keysize), _valuesize(other._valuesize),
    _keymax(other._keymax), _valuemax(other._valuemax) {
    // Creation and destruction of _group and _transaction is managed
    // by Python.
}

Operation::~Operation()
{
    // Creation and destruction of _group, _transaction is managed by Python.
}

void Operation::size_check() const {
    if (_optype != OP_NONE && _key._size == 0 && _table.options.key_size == 0)
        THROW("operation requires a key size");
    if (OP_HAS_VALUE(this) && _value._size == 0 &&
      _table.options.value_size == 0)
        THROW("operation requires a value size");
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
    size_check();
    if (_optype != OP_NONE) {
        kv_compute_max(true);
        if (OP_HAS_VALUE(this))
            kv_compute_max(false);
        kv_size_buffer(true, keysize);
        kv_size_buffer(false, valuesize);

        // Note: to support multiple contexts we'd need a generation
        // count whenever we execute.
        if (_table._context_count != 0 &&
          _table._context_count != env._context->_context_count)
            THROW("multiple Contexts not supported");
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

void Operation::kv_gen(bool iskey, uint64_t n, char *result) const {
    uint64_t max;
    int size;

    size = iskey ? _keysize : _valuesize;
    max = iskey ? _keymax : _valuemax;
    if (n > max)
        THROW((iskey ? "Key" : "Value") << " (" << n
          << ") too large for size (" << size << ")");
    workgen_u64_to_string_zf(n, result, size);
}

void Operation::kv_compute_max(bool iskey) {
    uint64_t max;
    int size;

    size = iskey ? _key._size : _value._size;
    if (size == 0)
        size = iskey ? _table.options.key_size : _table.options.value_size;

    if (iskey && size < 2)
        THROW("Key.size too small for table '" << _table._uri << "'");
    if (!iskey && size < 1)
        THROW("Value.size too small for table '" << _table._uri << "'");

    if (size > 1)
        max = power64(10, (size - 1)) - 1;
    else
        max = 0;

    if (iskey) {
        _keysize = size;
        _keymax = max;
    } else {
        _valuesize = size;
        _valuemax = max;
    }
}

void Operation::kv_size_buffer(bool iskey, size_t &maxsize) const {
    if (iskey) {
        if ((size_t)_keysize > maxsize)
            maxsize = _keysize;
    } else {
        if ((size_t)_valuesize > maxsize)
            maxsize = _valuesize;
    }
}

uint64_t Operation::get_key_recno(ThreadEnvironment &env, tint_t tint)
{
    uint64_t recno_count;
    uint32_t rand;

    recno_count = env._context->_recno[tint];
    if (recno_count == 0)
        // The file has no entries, returning 0 forces a WT_NOTFOUND return.
        return (0);
    rand = workgen_random(env._rand_state);
    return (rand % recno_count + 1);  // recnos are one-based.
}

int Operation::run(ThreadEnvironment &env) {
    Thread *thread = env._thread;
    Track *track;
    tint_t tint = _table._tint;
    WT_CURSOR *cursor = env._cursors[tint];
    WT_SESSION *session = thread->_session;
    WT_DECL_RET;
    uint64_t recno;
    char *buf;
    bool measure_latency;

    recno = 0;
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
    switch (_optype) {
    case OP_INSERT:
        track = &env._stats.insert;
        recno = workgen_atomic_add64(&env._context->_recno[tint], 1);
        break;
    case OP_REMOVE:
        track = &env._stats.remove;
        recno = get_key_recno(env, tint);
        break;
    case OP_SEARCH:
        track = &env._stats.read;
        recno = get_key_recno(env, tint);
        break;
    case OP_UPDATE:
        track = &env._stats.update;
        recno = get_key_recno(env, tint);
        break;
    case OP_NONE:
        recno = 0;
        track = NULL;
        break;
    }

    measure_latency = track != NULL && track->ops != 0 &&
      track->track_latency() &&
      (track->ops % env._workload->options.sample_rate == 0);

    timespec start;
    if (measure_latency)
        workgen_epoch(&start);

    if (_transaction != NULL) {
        if (env._in_transaction)
            THROW("nested transactions not supported");
        session->begin_transaction(session,
          _transaction->_begin_config.c_str());
        env._in_transaction = true;
    }
    if (_optype != OP_NONE) {
        buf = thread->_keybuf;
        kv_gen(true, recno, buf);
        cursor->set_key(cursor, buf);
        if (OP_HAS_VALUE(this)) {
            buf = thread->_valuebuf;
            kv_gen(false, recno, buf);
            cursor->set_value(cursor, buf);
        }
        switch (_optype) {
        case OP_INSERT:
            WT_ERR(cursor->insert(cursor));
            break;
        case OP_REMOVE:
            WT_ERR_NOTFOUND_OK(cursor->remove(cursor));
            break;
        case OP_SEARCH:
            ret = cursor->search(cursor);
            break;
        case OP_UPDATE:
            WT_ERR_NOTFOUND_OK(cursor->update(cursor));
            break;
        default:
            ASSERT(false);
        }
        if (ret != 0) {
            track = &env._stats.not_found;
            ret = 0;  // WT_NOTFOUND allowed.
        }
        cursor->reset(cursor);
    }
    if (measure_latency) {
        timespec stop;
        workgen_epoch(&stop);
        track->incr_with_latency(ts_us(stop - start));
    } else if (track != NULL)
        track->incr();

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

void Operation::get_static_counts(Stats &stats, int multiplier)
{
    switch (_optype) {
    case OP_NONE:
        break;
    case OP_INSERT:
        stats.insert.ops += multiplier;
        break;
    case OP_REMOVE:
        stats.remove.ops += multiplier;
        break;
    case OP_SEARCH:
        stats.read.ops += multiplier;
        break;
    case OP_UPDATE:
        stats.update.ops += multiplier;
        break;
    default:
        ASSERT(false);
    }
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
          i != _group->end(); i++)
            i->get_static_counts(stats, multiplier * _repeatgroup);
}

int Operation::open_all(WT_SESSION *session, ThreadEnvironment &env) {
    (void)env;
    if (_group != NULL)
        for (std::vector<Operation>::iterator i = _group->begin();
          i != _group->end(); i++)
            WT_RET(i->open_all(session, env));
    return (0);
}

Track::Track(bool latency_tracking) : ops(0), latency_ops(0), latency(0),
    last_latency_ops(0), last_latency(0), min_latency(0), max_latency(0),
    us(NULL), ms(NULL), sec(NULL) {
    track_latency(latency_tracking);
}

Track::Track(const Track &other) : ops(other.ops),
    latency_ops(other.latency_ops), latency(other.latency),
    last_latency_ops(other.last_latency_ops), last_latency(other.last_latency),
    min_latency(other.min_latency), max_latency(other.max_latency),
    us(NULL), ms(NULL), sec(NULL) {
    if (other.us != NULL) {
        us = new uint32_t[LATENCY_US_BUCKETS];
        ms = new uint32_t[LATENCY_MS_BUCKETS];
        sec = new uint32_t[LATENCY_SEC_BUCKETS];
        memcpy(us, other.us, sizeof(uint32_t) * LATENCY_US_BUCKETS);
        memcpy(ms, other.ms, sizeof(uint32_t) * LATENCY_MS_BUCKETS);
        memcpy(sec, other.sec, sizeof(uint32_t) * LATENCY_SEC_BUCKETS);
    }
}

Track::~Track() {
    if (us != NULL) {
        delete us;
        delete ms;
        delete sec;
    }
}

void Track::add(const Track &other) {
    ops += other.ops;
    latency_ops += other.latency_ops;
    latency += other.latency;
    last_latency_ops += other.last_latency_ops;
    last_latency += other.last_latency;

    min_latency = MIN(min_latency, other.min_latency);
    max_latency = MAX(max_latency, other.max_latency);

    if (us != NULL && other.us != NULL) {
        for (int i = 0; i < LATENCY_US_BUCKETS; i++)
            us[i] += other.us[i];
        for (int i = 0; i < LATENCY_MS_BUCKETS; i++)
            ms[i] += other.ms[i];
        for (int i = 0; i < LATENCY_SEC_BUCKETS; i++)
            sec[i] += other.sec[i];
    }
}

void Track::assign(const Track &other) {
    ops = other.ops;
    latency_ops = other.latency_ops;
    latency = other.latency;
    last_latency_ops = other.last_latency_ops;
    last_latency = other.last_latency;
    min_latency = other.min_latency;
    max_latency = other.max_latency;

    if (other.us == NULL && us != NULL) {
        delete us;
        delete ms;
        delete sec;
        us = NULL;
        ms = NULL;
        sec = NULL;
    }
    else if (other.us != NULL && us == NULL) {
        us = new uint32_t[LATENCY_US_BUCKETS];
        ms = new uint32_t[LATENCY_MS_BUCKETS];
        sec = new uint32_t[LATENCY_SEC_BUCKETS];
    }
    if (us != NULL) {
        memcpy(us, other.us, sizeof(uint32_t) * LATENCY_US_BUCKETS);
        memcpy(ms, other.ms, sizeof(uint32_t) * LATENCY_MS_BUCKETS);
        memcpy(sec, other.sec, sizeof(uint32_t) * LATENCY_SEC_BUCKETS);
    }
}

void Track::clear() {
    ops = 0;
    latency_ops = 0;
    latency = 0;
    last_latency_ops = 0;
    last_latency = 0;
    min_latency = 0;
    max_latency = 0;
    if (us != NULL) {
        memset(us, 0, sizeof(uint32_t) * LATENCY_US_BUCKETS);
        memset(ms, 0, sizeof(uint32_t) * LATENCY_MS_BUCKETS);
        memset(sec, 0, sizeof(uint32_t) * LATENCY_SEC_BUCKETS);
    }
}

void Track::incr() {
    ops++;
}

void Track::incr_with_latency(uint64_t usecs) {
    ASSERT(us != NULL);

    ops++;
    latency_ops++;
    latency += usecs;
    if (usecs > max_latency)
        max_latency = (uint32_t)usecs;
    if (usecs < min_latency)
        min_latency = (uint32_t)usecs;

    // Update a latency bucket.
    // First buckets: usecs from 100us to 1000us at 100us each.
    if (usecs < LATENCY_US_BUCKETS)
        us[usecs]++;

    // Second buckets: milliseconds from 1ms to 1000ms, at 1ms each.
    else if (usecs < ms_to_us(LATENCY_MS_BUCKETS))
        ms[us_to_ms(usecs)]++;

    // Third buckets are seconds from 1s to 100s, at 1s each.
    else if (usecs < sec_to_us(LATENCY_SEC_BUCKETS))
        sec[us_to_sec(usecs)]++;

    // >100 seconds, accumulate in the biggest bucket. */
    else
        sec[LATENCY_SEC_BUCKETS - 1]++;
}

void Track::subtract(const Track &other) {
    ops -= other.ops;
    latency_ops -= other.latency_ops;
    latency -= other.latency;
    last_latency_ops -= other.last_latency_ops;
    last_latency -= other.last_latency;

    // There's no sensible thing to be done for min/max_latency.

    if (us != NULL && other.us != NULL) {
        for (int i = 0; i < LATENCY_US_BUCKETS; i++)
            us[i] -= other.us[i];
        for (int i = 0; i < LATENCY_MS_BUCKETS; i++)
            ms[i] -= other.ms[i];
        for (int i = 0; i < LATENCY_SEC_BUCKETS; i++)
            sec[i] -= other.sec[i];
    }
}

void Track::track_latency(bool newval) {
    if (newval) {
        if (us == NULL) {
            us = new uint32_t[LATENCY_US_BUCKETS];
            ms = new uint32_t[LATENCY_MS_BUCKETS];
            sec = new uint32_t[LATENCY_SEC_BUCKETS];
            memset(us, 0, sizeof(uint32_t) * LATENCY_US_BUCKETS);
            memset(ms, 0, sizeof(uint32_t) * LATENCY_MS_BUCKETS);
            memset(sec, 0, sizeof(uint32_t) * LATENCY_SEC_BUCKETS);
        }
    } else {
        if (us != NULL) {
            delete us;
            delete ms;
            delete sec;
            us = NULL;
            ms = NULL;
            sec = NULL;
        }
    }
}

void Track::_get_us(long *result) {
    if (us != NULL) {
        for (int i = 0; i < LATENCY_US_BUCKETS; i++)
            result[i] = (long)us[i];
    } else
        memset(result, 0, sizeof(long) * LATENCY_US_BUCKETS);
}
void Track::_get_ms(long *result) {
    if (ms != NULL) {
        for (int i = 0; i < LATENCY_MS_BUCKETS; i++)
            result[i] = (long)ms[i];
    } else
        memset(result, 0, sizeof(long) * LATENCY_MS_BUCKETS);
}
void Track::_get_sec(long *result) {
    if (sec != NULL) {
        for (int i = 0; i < LATENCY_SEC_BUCKETS; i++)
            result[i] = (long)sec[i];
    } else
        memset(result, 0, sizeof(long) * LATENCY_SEC_BUCKETS);
}

Stats::Stats(bool latency) : insert(latency), not_found(latency),
    read(latency), remove(latency), update(latency), truncate(latency) {
}

Stats::Stats(const Stats &other) : insert(other.insert),
    not_found(other.not_found), read(other.read), remove(other.remove),
    update(other.update), truncate(other.truncate) {
}

Stats::~Stats() {}

void Stats::add(const Stats &other) {
    insert.add(other.insert);
    not_found.add(other.not_found);
    read.add(other.read);
    remove.add(other.remove);
    update.add(other.update);
    truncate.add(other.truncate);
}

void Stats::assign(const Stats &other) {
    insert.assign(other.insert);
    not_found.assign(other.not_found);
    read.assign(other.read);
    remove.assign(other.remove);
    update.assign(other.update);
    truncate.assign(other.truncate);
}

void Stats::clear() {
    insert.clear();
    not_found.clear();
    read.clear();
    remove.clear();
    update.clear();
    truncate.clear();
}

void Stats::describe(std::ostream &os) const {
    os << "Stats: reads " << read.ops;
    if (not_found.ops > 0) {
        os << " (" << not_found.ops << " not found)";
    }
    os << ", inserts " << insert.ops;
    os << ", updates " << update.ops;
    os << ", truncates " << truncate.ops;
    os << ", removes " << remove.ops;
}

void Stats::final_report(std::ostream &os, timespec &totalsecs) const {
    uint64_t ops = 0;
    ops += read.ops;
    ops += not_found.ops;
    ops += insert.ops;
    ops += update.ops;
    ops += truncate.ops;
    ops += remove.ops;

#define FINAL_OUTPUT(os, field, singular, ops, totalsecs)               \
    os << "Executed " << field << " " #singular " operations ("         \
       << PCT(field, ops) << "%) " << OPS_PER_SEC(field, totalsecs)     \
       << " ops/sec" << std::endl

    FINAL_OUTPUT(os, read.ops, read, ops, totalsecs);
    FINAL_OUTPUT(os, not_found.ops, not found, ops, totalsecs);
    FINAL_OUTPUT(os, insert.ops, insert, ops, totalsecs);
    FINAL_OUTPUT(os, update.ops, update, ops, totalsecs);
    FINAL_OUTPUT(os, truncate.ops, truncate, ops, totalsecs);
    FINAL_OUTPUT(os, remove.ops, remove, ops, totalsecs);
}

void Stats::report(std::ostream &os) const {
    os << read.ops << " reads";
    if (not_found.ops > 0) {
        os << " (" << not_found.ops << " not found)";
    }
    os << ", " << insert.ops << " inserts, ";
    os << update.ops << " updates, ";
    os << truncate.ops << " truncates, ";
    os << remove.ops << " removes";
}

void Stats::subtract(const Stats &other) {
    insert.subtract(other.insert);
    not_found.subtract(other.not_found);
    read.subtract(other.read);
    remove.subtract(other.remove);
    update.subtract(other.update);
    truncate.subtract(other.truncate);
}

void Stats::track_latency(bool latency) {
    insert.track_latency(latency);
    not_found.track_latency(latency);
    read.track_latency(latency);
    remove.track_latency(latency);
    update.track_latency(latency);
    truncate.track_latency(latency);
}

TableOptions::TableOptions() : key_size(0), value_size(0) {}
TableOptions::TableOptions(const TableOptions &other) :
    key_size(other.key_size), value_size(other.value_size) {}
TableOptions::~TableOptions() {}

Table::Table() : options(), _uri(), _tint(0), _context_count(0) {}
Table::Table(const char *uri) : options(), _uri(uri), _tint(0),
    _context_count(0) {}
Table::Table(const Table &other) : options(other.options), _uri(other._uri),
    _tint(other._tint), _context_count(other._context_count) {}
Table::~Table() {}

void Table::describe(std::ostream &os) const {
    os << "Table: " << _uri;
}

WorkloadOptions::WorkloadOptions() : report_interval(0), run_time(0),
    sample_interval(0), sample_rate(1) {}
WorkloadOptions::WorkloadOptions(const WorkloadOptions &other) :
    report_interval(other.report_interval), run_time(other.run_time),
    sample_interval(other.sample_interval), sample_rate(other.sample_rate) {}
WorkloadOptions::~WorkloadOptions() {}

Workload::Workload(Context *context, const ThreadListWrapper &tlw) :
    options(), stats(), _context(context), _threads(tlw._threads) {
    if (context == NULL)
        THROW("Workload contructor requires a Context");
}

Workload::Workload(Context *context, const Thread &thread) :
    options(), stats(), _context(context), _threads() {
    if (context == NULL)
        THROW("Workload contructor requires a Context");
    _threads.push_back(thread);
}

Workload::Workload(const Workload &other) :
    options(other.options), stats(other.stats), _context(other._context),
    _threads(other._threads) {
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
        envs[i]._workload = this;
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

    if (options.sample_interval > 0 && options.sample_rate <= 0)
        THROW("Workload.options.sample_rate must be positive");
    WT_ERR(create_all(conn, _context, envs));
    WT_ERR(open_all(envs));
    WT_ERR(ThreadEnvironment::cross_check(envs));
    WT_ERR(run_all(envs));

  err:
    //TODO: (void)close_all();
    return (ret);
}

void Workload::get_stats(std::vector<ThreadEnvironment> &envs,
  Stats &result) {
    for (size_t i = 0; i < _threads.size(); i++)
        result.add(envs[i]._stats);
}

void Workload::report(std::vector<ThreadEnvironment> &envs, time_t interval,
  time_t totalsecs, Stats &prev_totals) {
    Stats new_totals(prev_totals.track_latency());
    get_stats(envs, new_totals);
    Stats diff(new_totals);
    diff.subtract(prev_totals);
    prev_totals.assign(new_totals);
    diff.report(std::cout);
    std::cout << " in " << interval << " secs ("
              << totalsecs << " total secs)" << std::endl;
}

void Workload::final_report(std::vector<ThreadEnvironment> &envs,
  timespec &totalsecs) {
    stats.clear();
    stats.track_latency(options.sample_interval > 0);

    get_stats(envs, stats);
    stats.final_report(std::cout, totalsecs);
    std::cout << "Run completed: " << totalsecs << " seconds" << std::endl;
}

int Workload::run_all(std::vector<ThreadEnvironment> &envs) {
    void *status;
    std::vector<pthread_t> thread_handles;
    Stats counts(false);
    WorkgenException *exception;
    WT_DECL_RET;

    for (size_t i = 0; i < _threads.size(); i++)
        _threads[i].get_static_counts(counts);
    std::cout << "Starting workload: " << _threads.size() << " threads, ";
    counts.report(std::cout);
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

    Stats curstats(false);
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
            report(envs, options.report_interval, (now - start).tv_sec,
              curstats);
            while (now >= next_report)
                next_report += options.report_interval;
        }
    }
    if (options.run_time != 0)
        for (size_t i = 0; i < _threads.size(); i++)
            _threads[i]._stop = true;

    exception = NULL;
    for (size_t i = 0; i < _threads.size(); i++) {
        WT_TRET(pthread_join(thread_handles[i], &status));
        if (envs[i]._errno != 0)
            VERBOSE(envs[i],
                    "Thread " << i << " has errno " << envs[i]._errno);
        WT_TRET(envs[i]._errno);
        _threads[i].close_all(envs[i]);
        if (exception == NULL && !envs[i]._exception._str.empty())
            exception = &envs[i]._exception;
    }
    timespec finalsecs = now - start;
    final_report(envs, finalsecs);

    if (ret != 0)
        std::cerr << "run_all failed err=" << ret << std::endl;
    std::cout << std::endl;
    if (exception != NULL)
        throw *exception;
    return (ret);
}

};
