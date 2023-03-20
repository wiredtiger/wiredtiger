/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

/*
 * io_trace_kind --
 *     The type (kind) of the trace.
 */
enum class io_trace_kind {
    DEVICE,
    FILE,
    WIRED_TIGER,
};

/*
 * io_trace_operation --
 *     A single data point within a trace.
 */
struct io_trace_operation {

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
     *     Wrap a timestamp into an instance of io_trace_operation, which is useful for filtering a
     * collection of traces.
     */
    static io_trace_operation
    wrap_timestamp(double t)
    {
        io_trace_operation r;
        bzero(&r, sizeof(r));
        r.timestamp = t;
        return r;
    }

    /*
     * operator< --
     *     The "<" comparision operation.
     */
    inline bool
    operator<(const io_trace_operation &other) const
    {
        return timestamp < other.timestamp;
    }

    /*
     * operator> --
     *     The ">" comparision operation.
     */
    inline bool
    operator>(const io_trace_operation &other) const
    {
        return timestamp > other.timestamp;
    }
};

class io_trace_collection;

/*
 * io_trace --
 *     A trace from the same device.
 */
class io_trace {

    friend class io_trace_collection;

public:
    io_trace(io_trace_collection &parent, const char *name);
    virtual ~io_trace();

    /*
     * name --
     *     Get the name of the trace.
     */
    inline const char *
    name() const
    {
        return _name.c_str();
    }

    /*
     * operations --
     *     Get the vector of operations.
     */
    inline const std::vector<io_trace_operation> &
    operations() const
    {
        return _operations;
    }

protected:
    io_trace_collection &_parent;
    std::string _name;
    std::vector<io_trace_operation> _operations;
};

/*
 * io_trace_collection --
 *     A collection of related traces (i.e., from the same workload run).
 */
class io_trace_collection {

public:
    io_trace_collection();
    virtual ~io_trace_collection();

    void load_from_file(const char *file);

    /*
     * traces --
     *     Get the (sorted) map of names to traces.
     */
    inline const std::map<std::string, io_trace *> &
    traces() const
    {
        return _traces;
    }

protected:
    std::map<std::string, io_trace *> _traces;

    void add_data_point(
      const std::string &device_or_file, const io_trace_kind kind, const io_trace_operation &item);

private:
    void load_from_file_blkparse(FILE *f);
    void load_from_file_wt_logs(FILE *f);
};
