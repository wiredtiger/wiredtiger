#!/usr/bin/env python3

import os, log_data
from dist import compare_srcfile, format_srcfile

# Temporary file.
tmp_file = '__tmp_log' + str(os.getpid())

# Map log record types to:
#   0. C type
#   1. pack type
#   2. printf format, or a list of printf formats (one regular, and optionally one more for hex)
#   3. printf arg(s)
#   4. list of setup functions (one regular, and optionally one more for hex)
#   5. always include hex (regardless of whether WT_TXN_PRINTLOG_HEX was set)
#   6. passed by pointer
field_types = {
    'WT_LSN' : ('WT_LSN', 'II', '[%" PRIu32 ", %" PRIu32 "]',
        'arg.l.file, arg.l.offset', [ '' ], False, True),
    'string' : ('char *', 'S', '\\"%s\\"', 'arg', [ '' ], False, False),
    'WT_ITEM' : ('WT_ITEM', 'u', '\\"%s\\"', '(char *)escaped->mem',
        [ 'WT_ERR(__logrec_make_json_str(session, &escaped, &arg));',
          'WT_ERR(__logrec_make_hex_str(session, &escaped, &arg));'], False, True),
    'recno' : ('uint64_t', 'r', '%" PRIu64 "', 'arg', [ '' ], False, False),
    'uint32_t' : ('uint32_t', 'I', '%" PRIu32 "', 'arg', [ '' ], False, False),
    # The fileid may have the high bit set. Print in both decimal and hex.
    'uint32_id' : ('uint32_t', 'I',
        [ '%" PRIu32 "', '\\"0x%" PRIx32 "\\"' ], 'arg', [ '', '' ], True, False),
    'uint64_t' : ('uint64_t', 'Q', '%" PRIu64 "', 'arg', [ '' ], False, False),
}

def cintype(f):
    return field_types[f[0]][0] + ('*' if field_types[f[0]][6] else ' ')

def couttype(f):
    return field_types[f[0]][0] + '*'

def clocaltype(f):
    return field_types[f[0]][0]

def escape_decl(fields):
    return '\n\tWT_DECL_ITEM(escaped);' if has_escape(fields) else ''

def has_escape(fields):
    for f in fields:
        for setup in field_types[f[0]][4]:
            if 'escaped' in setup:
                return True
    return False

def pack_fmt(fields):
    return ''.join(field_types[f[0]][1] for f in fields)

def op_pack_fmt(r):
    return 'II' + pack_fmt(r.fields)

def rec_pack_fmt(r):
    return 'I' + pack_fmt(r.fields)

def printf_fmt(f, ishex):
    fmt = field_types[f[0]][2]
    if type(fmt) is list:
        fmt = fmt[ishex]
    return fmt

def pack_arg(f):
    if f[0] == 'WT_LSN':
        return '%s->l.file, %s->l.offset' % (f[1], f[1])
    return f[1]

def printf_arg(f):
    arg = field_types[f[0]][3].replace('arg', f[1])
    return ' ' + arg

def unpack_arg(f):
    if f[0] == 'WT_LSN':
        return '&%sp->l.file, &%sp->l.offset' % (f[1], f[1])
    return f[1] + 'p'

def printf_setup(f, i, nl_indent):
    stmt = field_types[f[0]][4][i].replace('arg', f[1])
    return '' if stmt == '' else stmt + nl_indent

def n_setup(f):
    return len(field_types[f[0]][4])

def unconditional_hex(f):
    return field_types[f[0]][5]

# Check for an operation that has a file id type. Redact any user data
# if the redact flag is set, but print operations for file id 0, known
# to be the metadata.
def check_redact(optype):
    for f  in optype.fields:
        if f[0] == 'uint32_id':
            redact_str = '\tif (!FLD_ISSET(args->flags, WT_TXN_PRINTLOG_UNREDACT) && '
            redact_str += '%s != WT_METAFILE_ID)\n' % (f[1])
            redact_str += '\t\treturn(__wt_fprintf(session, args->fs, " REDACTED"));\n'
            return redact_str
    return ''


def struct_size_body(f, var):
    # if f[0] == 'WT_ITEM' or f[0] == 'WT_LSN':
    #     return ''
    if f[0] == 'WT_ITEM':
        return '__wt_vsize_uint((%(name)s)->size) + (%(name)s)->size' % {'name' : var}
    else:
        return '__wt_vsize_uint(%(name)s)' % {'name' : var}

def struct_pack_body(f, var):
    # if f[0] == 'WT_ITEM' or f[0] == 'WT_LSN':
    #     return ''
    funcname = '__pack_encode__WT_ITEM' if f[0] == 'WT_ITEM' else '__pack_encode__uintAny';
    return '''\tWT_RET(%(funcname)s(pp, end, %(name)s));
''' % {'funcname': funcname, 'name' : var}

def struct_unpack_body(f, var):
    # if f[0] == 'WT_ITEM' or f[0] == 'WT_LSN':
    #     return ''
    funcname = '__pack_decode__WT_ITEM' if f[0] == 'WT_ITEM' else '__pack_decode__uintAny';
    return '''%(funcname)s(%(type)s, %(name)sp);
''' % {'funcname': funcname, 'name' : var, 'type': cintype(f)}


# Create a printf line, with an optional setup function.
# ishex indicates that the the field name in the output is modified
# (to add "-hex"), and that the setup and printf are conditional
# in the generated code.
def printf_line(f, optype, i, ishex):
    ifbegin = ''
    ifend = ''
    nl_indent = '\n\t'
    name = f[1]
    postcomma = '' if i + 1 == len(optype.fields) else ',\\n'
    precomma = ''
    if ishex > 0:
        name += '-hex'
        if not unconditional_hex(f):
            ifend = nl_indent + '}'
            nl_indent += '\t'
            ifbegin = \
                'if (FLD_ISSET(args->flags, WT_TXN_PRINTLOG_HEX)) {' + nl_indent
            if postcomma == '':
                precomma = ',\\n'
    body = '%s%s(__wt_fprintf(session, args->fs,' % (
        printf_setup(f, ishex, nl_indent),
        'WT_ERR' if has_escape(optype.fields) else 'WT_RET') + \
        '%s    "%s        \\"%s\\": %s%s",%s));' % (
        nl_indent, precomma, name, printf_fmt(f, ishex), postcomma,
        printf_arg(f))
    return ifbegin + body + ifend

#####################################################################
# Create log_auto.c with handlers for each record / operation type.
#####################################################################
f='../src/log/log_auto.c'
tfile = open(tmp_file, 'w')

tfile.write('/* DO NOT EDIT: automatically built by dist/log.py. */\n')

tfile.write('''
#include "wt_internal.h"

#define WT_SIZE_CHECK_PACK_PTR(p, end) WT_RET_TEST((p) && (end) && (p) < (end), ENOMEM)
#define WT_SIZE_CHECK_UNPACK_PTR(p, end) WT_RET_TEST((p) && (end) && (p) < (end), EINVAL)

/*
 * __pack_encode__WT_ITEM --
 *\tPack a WT_ITEM structure.
 */

static inline int
__pack_encode__WT_ITEM(uint8_t **pp, uint8_t *end, WT_ITEM *item)
{
    WT_RET(__wt_vpack_uint(pp, WT_PTRDIFF(end, *pp), item->size));
    WT_SIZE_CHECK_PACK(item->size, WT_PTRDIFF(end, *pp));
    memcpy(*pp, item->data, item->size);
    *pp += item->size;
    return (0);
}

static inline int
__pack_encode__uintAny(uint8_t **pp, uint8_t *end, uint64_t item)
{
    /* Check that there is at least one byte available: the low-level routines treat zero length as unchecked. */
    WT_SIZE_CHECK_PACK_PTR(*pp, end);
    return __wt_vpack_uint(pp, WT_PTRDIFF(end, *pp), item);
}

#define __pack_decode__uintAny(TYPE, pval)  do { \
        uint64_t v; \
        /* Check that there is at least one byte available: the low-level routines treat zero length as unchecked. */ \
        WT_SIZE_CHECK_UNPACK_PTR(*pp, end); \
        WT_RET(__wt_vunpack_uint(pp, WT_PTRDIFF(end, *pp), &v)); \
        *(pval) = (TYPE)v; \
    } while (0)

#define __pack_decode__WT_ITEM(TYPE, val)  do { \
        __pack_decode__uintAny(size_t, &val->size); \
        WT_SIZE_CHECK_UNPACK(val->size, WT_PTRDIFF(end, *pp)); \
        val->data = *pp; \
        *pp += val->size; \
    } while (0)

/*
 * __wt_logrec_alloc --
 *\tAllocate a new WT_ITEM structure.
 */
int
__wt_logrec_alloc(WT_SESSION_IMPL *session, size_t size, WT_ITEM **logrecp)
{
\tWT_ITEM *logrec;
\tWT_LOG *log;

\tlog = S2C(session)->log;
\tWT_RET(
\t    __wt_scr_alloc(session, WT_ALIGN(size + 1, log->allocsize), &logrec));
\tWT_CLEAR(*(WT_LOG_RECORD *)logrec->data);
\tlogrec->size = offsetof(WT_LOG_RECORD, record);

\t*logrecp = logrec;
\treturn (0);
}

/*
 * __wt_logrec_free --
 *\tFree the given WT_ITEM structure.
 */
void
__wt_logrec_free(WT_SESSION_IMPL *session, WT_ITEM **logrecp)
{
\t__wt_scr_free(session, logrecp);
}

/*
 * __wt_logrec_read --
 *\tRead the record type.
 */
int
__wt_logrec_read(WT_SESSION_IMPL *session,
    const uint8_t **pp, const uint8_t *end, uint32_t *rectypep)
{
\tWT_UNUSED(session);
\t__pack_decode__uintAny(uint32_t, rectypep);
\treturn (0);
}

/*
 * __wt_logrec_write --
 *\tWrite the record type.
 */
int
__wt_logrec_write(WT_SESSION_IMPL *session,
    uint8_t **pp, uint8_t *end, uint32_t rectype)
{
\tWT_UNUSED(session);
\tWT_RET(__pack_encode__uintAny(pp, end, rectype));
\treturn (0);
}

/*
 * __wt_logop_read --
 *\tRead the operation type.
 */
int
__wt_logop_read(WT_SESSION_IMPL *session,
    const uint8_t **pp, const uint8_t *end,
    uint32_t *optypep, uint32_t *opsizep)
{
\treturn (__wt_struct_unpack(
\t    session, *pp, WT_PTRDIFF(end, *pp), "II", optypep, opsizep));
\t__pack_decode__uintAny(uint32_t, optypep);
\t__pack_decode__uintAny(uint32_t, opsizep);
\treturn (0);
}

/*
 * __wt_logop_write --
 *\tWrite the operation type.
 */
int
__wt_logop_write(WT_SESSION_IMPL *session,
    uint8_t **pp, uint8_t *end,
    uint32_t optype, uint32_t opsize)
{
\tWT_UNUSED(session);
\tWT_RET(__pack_encode__uintAny(pp, end, optype));
\tWT_RET(__pack_encode__uintAny(pp, end, opsize));
\treturn (0);
}

/*
 * __logrec_make_json_str --
 *\tUnpack a string into JSON escaped format.
 */
static int
__logrec_make_json_str(WT_SESSION_IMPL *session, WT_ITEM **escapedp, WT_ITEM *item)
{
\tsize_t needed;

\tneeded = (item->size * WT_MAX_JSON_ENCODE) + 1;

\tif (*escapedp == NULL)
\t\tWT_RET(__wt_scr_alloc(session, needed, escapedp));
\telse
\tWT_RET(__wt_buf_grow(session, *escapedp, needed));
\tWT_IGNORE_RET(
\t\t__wt_json_unpack_str((*escapedp)->mem, (*escapedp)->memsize, item->data, item->size));
\treturn (0);
}

/*
 * __logrec_make_hex_str --
 *\tConvert data to a hexadecimal representation.
 */
static int
__logrec_make_hex_str(WT_SESSION_IMPL *session, WT_ITEM **escapedp, WT_ITEM *item)
{
\tsize_t needed;

\tneeded = (item->size * 2) + 1;

\tif (*escapedp == NULL)
\t\tWT_RET(__wt_scr_alloc(session, needed, escapedp));
\telse
\tWT_RET(__wt_buf_grow(session, *escapedp, needed));
\t__wt_fill_hex(item->data, item->size, (*escapedp)->mem, (*escapedp)->memsize, NULL);
\treturn (0);
}

''')

# Emit code to read, write and print log operations (within a log record)
for optype in log_data.optypes:
    tfile.write('''
/*
 * __wt_struct_size_%(name)s --
 *\tCalculate size of %(name)s struct.
 */
static inline void
__wt_struct_size_%(name)s(size_t *sizep%(comma)s
    %(arg_decls_in)s)
{
    *sizep = %(size_body)s;
    return;
}


/*
 * __wt_struct_pack_%(name)s --
 *\tPack the %(name)s struct.
 */
WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result))
static inline int
__wt_struct_pack_%(name)s(uint8_t **pp, uint8_t *end%(comma)s
    %(arg_decls_in)s)
{
    %(pack_body)s
    return (0);
}

/*
 * __wt_struct_unpack_%(name)s --
 *\tUnpack the %(name)s struct.
 */
WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result))
static inline int
__wt_struct_unpack_%(name)s(const uint8_t **pp, const uint8_t *end%(comma)s
    %(arg_decls_out)s)
{
    %(unpack_body)s
    return (0);
}


/*
 * __wt_logop_%(name)s_pack --
 *\tPack the log operation %(name)s.
 */
WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result))
int
__wt_logop_%(name)s_pack(
    WT_SESSION_IMPL *session, WT_ITEM *logrec%(comma)s
    %(arg_decls_in)s)
{
\tsize_t size;
\tuint8_t *buf, *end;

\t__wt_struct_size_%(name)s(&size%(comma)s%(pack_args)s);
\tsize += __wt_vsize_uint(%(macro)s) + __wt_vsize_uint(0);
\t__wt_struct_size_adjust(session, &size);
\tWT_RET(__wt_buf_extend(session, logrec, logrec->size + size));
\tbuf = (uint8_t *)logrec->data + logrec->size;
\tend = buf + size;
\tWT_RET(__wt_logop_write(session, &buf, end, %(macro)s, (uint32_t)size));
\tWT_RET(__wt_struct_pack_%(name)s(&buf, end%(comma)s%(pack_args)s));

\tlogrec->size += (uint32_t)size;
\treturn (0);
}

/*
 * __wt_logop_%(name)s_unpack --
 *\tUnpack the log operation %(name)s.
 */
int
__wt_logop_%(name)s_unpack(
    WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end%(comma)s
    %(arg_decls_out)s)
{
\tWT_DECL_RET;
\tuint32_t optype, size;

#ifdef HAVE_DIAGNOSTIC  /* This is when WT_ASSERT is enabled. */
\tconst uint8_t **pp_orig;
\tpp_orig = pp;
#endif

\tif ((ret = __wt_logop_read(session, pp, end, &optype, &size)) != 0 ||
\t\t\t(ret = __wt_struct_unpack_%(name)s(pp, end%(comma)s%(unpack_args)s)) != 0)
\t\tWT_RET_MSG(session, ret, "logop_%(name)s: unpack failure");

\tWT_ASSERT(session, optype == %(macro)s);
#ifdef HAVE_DIAGNOSTIC  /* This is when WT_ASSERT is enabled. */
\tWT_ASSERT(session, WT_PTRDIFF(end, pp_orig) >= size);
#endif

\t*pp += size;
\treturn (0);
}
''' % {
    'name' : optype.name,
    'macro' : optype.macro_name,
    'comma' : ',' if optype.fields else '',
    'arg_decls_in' : ', '.join(
        '%s%s%s' % (cintype(f), '' if cintype(f)[-1] == '*' else ' ', f[1])
        for f in optype.fields),
    'arg_decls_out' : ', '.join(
        '%s%sp' % (couttype(f), f[1]) for f in optype.fields),
    'size_body' : ' + '.join(struct_size_body(f, f[1]) for f in optype.fields) if optype.fields else '0',
    'pack_args' : ', '.join(pack_arg(f) for f in optype.fields),
    'pack_body' : ''.join(struct_pack_body(f, f[1]) for f in optype.fields) if optype.fields else '\tWT_UNUSED(pp);\n\tWT_UNUSED(end);',
    'unpack_args' : ', '.join(unpack_arg(f) for f in optype.fields),
    'unpack_body' : ''.join(struct_unpack_body(f, f[1]) for f in optype.fields) if optype.fields else '\tWT_UNUSED(pp);\n\tWT_UNUSED(end);',
    'fmt' : op_pack_fmt(optype),
})

    tfile.write('''
/*
 * __wt_logop_%(name)s_print --
 *\tPrint the log operation %(name)s.
 */
int
__wt_logop_%(name)s_print(WT_SESSION_IMPL *session,
    const uint8_t **pp, const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args)
{%(arg_ret)s%(arg_decls)s

\tWT_RET(__wt_logop_%(name)s_unpack(session, pp, end%(arg_addrs)s));

\t%(redact)s
\tWT_RET(__wt_fprintf(session, args->fs,
\t    " \\"optype\\": \\"%(name)s\\"%(comma)s\\n"));
%(print_args)s
%(arg_fini)s
}
''' % {
    'name' : optype.name,
    'comma' : ',' if len(optype.fields) > 0 else '',
    'arg_ret' : ('\n\tWT_DECL_RET;' if has_escape(optype.fields) else ''),
    'arg_decls' : (('\n\t' + '\n\t'.join('%s%s%s;' %
        (clocaltype(f), '' if clocaltype(f)[-1] == '*' else ' ', f[1])
        for f in optype.fields)) + escape_decl(optype.fields)
        if optype.fields else ''),
    'arg_fini' : ('\nerr:\t__wt_scr_free(session, &escaped);\n\treturn (ret);'
    if has_escape(optype.fields) else '\treturn (0);'),
    'arg_addrs' : ''.join(', &%s' % f[1] for f in optype.fields),
    'redact' : check_redact(optype),
    'print_args' : ('\t' + '\n\t'.join(printf_line(f, optype, i, s)
        for i,f in enumerate(optype.fields) for s in range(0, n_setup(f)))
        if optype.fields else ''),
})

# Emit the printlog entry point
tfile.write('''
/*
 * __wt_txn_op_printlog --
 *\tPrint operation from a log cookie.
 */
int
__wt_txn_op_printlog(WT_SESSION_IMPL *session,
    const uint8_t **pp, const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args)
{
\tuint32_t optype, opsize;

\t/* Peek at the size and the type. */
\tWT_RET(__wt_logop_read(session, pp, end, &optype, &opsize));
\tend = *pp + opsize;

\tswitch (optype) {''')

for optype in log_data.optypes:
    tfile.write('''
\tcase %(macro)s:
\t\tWT_RET(%(print_func)s(session, pp, end, args));
\t\tbreak;
''' % {
    'macro' : optype.macro_name,
    'print_func' : '__wt_logop_' + optype.name + '_print',
})

tfile.write('''
\tdefault:\n\t\treturn (__wt_illegal_value(session, optype));
\t}

\treturn (0);
}
''')

tfile.close()
format_srcfile(tmp_file)
compare_srcfile(tmp_file, f)
