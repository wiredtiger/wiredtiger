/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef BLOCK_TRACE_EXPLORER_TRACE_H
#define BLOCK_TRACE_EXPLORER_TRACE_H

#include <map>
#include <string>
#include <vector>

/*
 * TraceKind --
 *     The type (kind) of the trace.
 */
enum class TraceKind {
    DEVICE,
    FILE,
    WIRED_TIGER,
};

/*
 * TraceOperation --
 *     A single data point within a trace.
 */
struct TraceOperation {

    double timestamp; /* The timestamp in seconds, relative to the start of the trace. */
    char action;      /* The action, as defined by blktrace, plus a few custom actions. */

    bool read;        /* Was this a read? */
    bool write;       /* Was this a write? */
    bool synchronous; /* Whether this was a synchronous operation. */
    bool barrier;     /* Whether this included a barrier operation. */
    bool discard;     /* Whether this was a discard operation. */

    unsigned long offset; /* Offset in bytes from the beginning of the file or the device. */
    unsigned length;      /* Length in bytes. */
    double duration;      /* Duration in seconds, if available (or 0 if not). */
    char process[32];     /* The issuing process name, if available. */

    /*
     * wrap_timestamp --
     *     Wrap a timestamp into an instance of TraceOperation, which is useful for filtering a
     * collection of traces.
     */
    static TraceOperation
    wrap_timestamp(double t)
    {
        TraceOperation r;
        bzero(&r, sizeof(r));
        r.timestamp = t;
        return r;
    }

    /*
     * operator< --
     *     The "<" comparision operation.
     */
    inline bool
    operator<(const TraceOperation &other) const
    {
        return timestamp < other.timestamp;
    }

    /*
     * operator> --
     *     The ">" comparision operation.
     */
    inline bool
    operator>(const TraceOperation &other) const
    {
        return timestamp > other.timestamp;
    }
};

class TraceCollection;

/*
 * Trace --
 *     A trace from the same device.
 */
class Trace {

    friend class TraceCollection;

public:
    Trace(TraceCollection &parent, const char *name);
    virtual ~Trace();

    /*
     * name --
     *     Get the name of the trace.
     */
    inline const char *
    name() const
    {
        return m_name.c_str();
    }

    /*
     * operations --
     *     Get the vector of operations.
     */
    inline const std::vector<TraceOperation> &
    operations() const
    {
        return m_operations;
    }

protected:
    TraceCollection &m_parent;
    std::string m_name;
    std::vector<TraceOperation> m_operations;
};

/*
 * TraceCollection --
 *     A collection of related traces (i.e., from the same workload run).
 */
class TraceCollection {

public:
    TraceCollection();
    virtual ~TraceCollection();

    void load_from_file(const char *file);

    /*
     * traces --
     *     Get the (sorted) map of names to traces.
     */
    inline const std::map<std::string, Trace *> &
    traces() const
    {
        return m_traces;
    }

protected:
    std::map<std::string, Trace *> m_traces;

    void add_data_point(
      const std::string &device_or_file, const TraceKind kind, const TraceOperation &item);

private:
    void load_from_file_blkparse(FILE *f);
    void load_from_file_wt_logs(FILE *f);
};

#endif /* BLOCK_TRACE_EXPLORER_TRACE_H */
