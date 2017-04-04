#include <iostream>
#include <sstream>
#include <pthread.h>
#include "wiredtiger.h"
#include "workgen.h"
//#include "test_util.h"   // TODO: cannot use yet, it includes wt_internal.h
extern "C" {
// Include specific files, as some files included by wt_internal.h
// have some C-ism's that don't work in C++.
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
    std::cout << "Executing the workload" << std::endl;
    workload.describe(std::cout);
    workload.run(wt_conn);

    return (0);
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

Thread::Thread() : _ops(), _name(), _session(NULL), _keybuf(NULL),
    _valuebuf(NULL) {
}

Thread::Thread(const std::vector<Operation> &ops) : _ops(ops), _name(),
    _session(NULL), _keybuf(NULL), _valuebuf(NULL) {
}

Thread::Thread(const Thread &other) : _ops(other._ops), _name(other._name),
    _session(NULL), _keybuf(NULL), _valuebuf(NULL) {
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
    for (std::vector<Operation>::const_iterator i = _ops.begin(); i != _ops.end(); i++) {
        WT_TRET(i->run(context));
        if (ret != 0) {
            std::cerr << "thread " << name << " failed err="
                      << ret << std::endl;
        }
    }
    VERBOSE(context, "thread " << name << "finished");
    return (ret);
}

Operation::Operation(const Operation &other) : _optype(other._optype),
  _table(other._table), _key(other._key), _value(other._value),
  _children(other._children), _repeatchildren(other._repeatchildren) {
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

int Operation::run(WorkgenContext &context) const {
    WT_CURSOR *cursor = _table._cursor;
    char *buf;

    // TODO: what happens if multiple threads choose the same keys
    // for a table.  Should separate thread-specific key spaces be
    // carved out?
    if (_optype != OP_NONE) {
        buf = context._thread->_keybuf;
        _key.gen(context._nrecords, buf);
        cursor->set_key(cursor, buf);
        switch (_optype) {
        case OP_INSERT:
            buf = context._thread->_valuebuf;
            _value.gen(context._nrecords, buf);
            cursor->set_value(cursor, buf);
            WT_RET(cursor->insert(cursor));
            context._nrecords++;
            break;
        case OP_REMOVE:
            WT_RET(cursor->remove(cursor));
            break;
        case OP_SEARCH:
            WT_RET(cursor->search(cursor));
            break;
        case OP_UPDATE:
            buf = context._thread->_valuebuf;
            _value.gen(context._nrecords, buf);
            cursor->set_value(cursor, buf);
            WT_RET(cursor->update(cursor));
            break;
        default:
            ASSERT(false);
        }
    }
    if (_children != NULL)
        for (int count = 0; count < _repeatchildren; count++)
            for (std::vector<Operation>::const_iterator i = _children->begin(); i != _children->end(); i++)
                WT_RET(i->run(context));
    return (0);
}

int Operation::open_all(WT_SESSION *session) {
    if (_optype != OP_NONE)
        WT_RET(_table.open_all(session));
    if (_children != NULL)
        for (std::vector<Operation>::iterator i = _children->begin(); i != _children->end(); i++)
            WT_RET(i->open_all(session));
    return (0);
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
        _threads[i]._name = sstm;
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

int Workload::run_all(std::vector<WorkgenContext> &contexts) {
    void *status;
    std::vector<pthread_t> thread_handles(_threads.size());
    WT_DECL_RET;

    for (int i = 0; i < (int)_threads.size(); i++)
        // TODO: on error, clean up already started threads, perhaps
        // each thread should have a 'okay' flag that can be set to false here.
        WT_RET(pthread_create(&thread_handles[i], NULL, thread_runner,
            &contexts[i]));

    for (int i = 0; i < (int)_threads.size(); i++) {
        WT_TRET(pthread_join(thread_handles[i], &status));
        if (contexts[i]._errno != 0)
            VERBOSE(contexts[i],
                    "Thread " << i << " has errno " << contexts[i]._errno);
        WT_TRET(contexts[i]._errno);
        _threads[i].close_all();
    }
    if (ret != 0)
        std::cerr << "run_all failed err=" << ret << std::endl;
    return (ret);
}

};
