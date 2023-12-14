#!/usr/bin/env python3

import os, log_data
from dist import compare_srcfile, format_srcfile

# Temporary file.
tmp_file = '__tmp_log' + str(os.getpid())

field_types = {}

class FieldType:
    def __init__(self, typename, ctype, packtype, printf_fmt_templ, printf_arg_templ, setup, always_hex, byptr):
        self.typename = typename                  # Internal type id
        self.ctype = ctype                        # C type
        self.packtype = packtype                  # pack type
        self.printf_fmt_templ = printf_fmt_templ  # printf format or list of printf formats
        self.printf_arg_templ = printf_arg_templ  # printf arg
        self.setup = setup                        # list of setup functions
        self.always_hex = always_hex              # always include hex
        self.byptr = byptr                        # passed by pointer

    @staticmethod
    def Add(*args):
        field_types[args[0]] = FieldType(*args)

FieldType.Add('WT_LSN',
    'WT_LSN', 'II', '[%" PRIu32 ", %" PRIu32 "]',
    'arg.l.file, arg.l.offset', [ '' ], False, True),
FieldType.Add('string',
    'const char *', 'S', '\\"%s\\"', 'arg', [ '' ], False, False),
FieldType.Add('WT_ITEM',
    'WT_ITEM', 'u', '\\"%s\\"', '(char *)escaped->mem',
    [ 'WT_ERR(__logrec_make_json_str(session, &escaped, &arg));',
      'WT_ERR(__logrec_make_hex_str(session, &escaped, &arg));'], False, True),
FieldType.Add('recno',
    'uint64_t', 'r', '%" PRIu64 "', 'arg', [ '' ], False, False),
FieldType.Add('uint32_t',
    'uint32_t', 'I', '%" PRIu32 "', 'arg', [ '' ], False, False),
# The fileid may have the high bit set. Print in both decimal and hex.
FieldType.Add('uint32_id',
    'uint32_t', 'I', [ '%" PRIu32 "', '\\"0x%" PRIx32 "\\"' ], 'arg', [ '', '' ], True, False),
FieldType.Add('uint64_t',
    'uint64_t', 'Q', '%" PRIu64 "', 'arg', [ '' ], False, False),


class Field:
    def __init__(self, field_tuple):
        # Copy all attributes from FieldType object into this object.
        self.__dict__.update(field_types[field_tuple[0]].__dict__)
        self.fieldname = field_tuple[1]

        self.cintype = self.ctype + ('*' if self.byptr else '')
        self.cindecl = self.ctype + (' *' if self.byptr else ' ') + self.fieldname
        self.couttype = self.ctype + '*'
        self.coutdecl = self.ctype + ' *' + self.fieldname + 'p'
        self.clocaldef = self.ctype + ' ' + self.fieldname
        self.n_setup = len(self.setup)
        self.printf_arg = ' ' + self.printf_arg_templ.replace('arg', self.fieldname)

        # Override functions for this type.
        for func in ['pack_arg', 'unpack_arg',
                     'struct_size_body', 'struct_pack_body', 'struct_unpack_body']:
            if (func + '__' + self.typename) in dir(self):
                setattr(self, func, getattr(self, func + '__' + self.typename))

    def printf_fmt(self, ishex):
        fmt = self.printf_fmt_templ
        if type(fmt) is list:
            fmt = fmt[ishex]
        return fmt

    def pack_arg(self):
        return self.fieldname
    # def pack_arg__WT_LSN(self):
    #     return '%(name)s->l.file, %(name)s->l.offset' % {'name' : self.fieldname}

    def unpack_arg(self):
        return self.fieldname + 'p'
    # def unpack_arg__WT_LSN(self):
    #     return '&%(name)sp->l.file, &%(name)sp->l.offset' % {'name' : self.fieldname}

    def printf_setup(self, i, nl_indent):
        stmt = self.setup[i].replace('arg', self.fieldname)
        return '' if stmt == '' else stmt + nl_indent

    def struct_size_body(self):
        return '__wt_vsize_uint(%s)' % self.fieldname
    def struct_size_body__WT_LSN(self):
        return '__wt_vsize_uint(%(name)s->l.file) + __wt_vsize_uint(%(name)s->l.offset)' % {'name' : self.fieldname}
    def struct_size_body__WT_ITEM(self):
        return '__wt_vsize_uint(%(name)s->size) + %(name)s->size' % {'name' : self.fieldname}
    def struct_size_body__string(self):
        return 'strlen(%s) + 1' % self.fieldname

    def struct_pack_body(self):
        return '\tWT_RET(__pack_encode__uintAny(pp, end, %s));\n' % self.fieldname
    def struct_pack_body__WT_LSN(self):
        return '\tWT_RET(__pack_encode__uintAny(pp, end, %(name)s->l.file));\n\tWT_RET(__pack_encode__uintAny(pp, end, %(name)s->l.offset));\n'  % {'name' : self.fieldname}
    def struct_pack_body__WT_ITEM(self):
        return '\tWT_RET(__pack_encode__WT_ITEM(pp, end, %s));\n' % self.fieldname
    def struct_pack_body__string(self):
        return '\tWT_RET(__pack_encode__string(pp, end, %s));\n' % self.fieldname

    def struct_unpack_body(self):
        return '__pack_decode__uintAny(%s, %sp);\n' % (self.cintype, self.fieldname)
    def struct_unpack_body__WT_LSN(self):
        return '__pack_decode__uintAny(uint32_t, &%(name)sp->l.file);__pack_decode__uintAny(uint32_t, &%(name)sp->l.offset);\n' % {'name':self.fieldname}
    def struct_unpack_body__WT_ITEM(self):
        return '__pack_decode__WT_ITEM(%sp);\n' % self.fieldname
    def struct_unpack_body__string(self):
        return '__pack_decode__string(%sp);\n' % self.fieldname


for op in log_data.optypes:
    op.fields = [ Field(f) for f in op.fields ]


def escape_decl(fields):
    return '\n\tWT_DECL_ITEM(escaped);' if has_escape(fields) else ''

def has_escape(fields):
    for f in fields:
        for setup in f.setup:
            if 'escaped' in setup:
                return True
    return False

def pack_fmt(fields):
    return ''.join(f.packtype for f in fields)

def op_pack_fmt(r):
    return 'II' + pack_fmt(r.fields)

def rec_pack_fmt(r):
    return 'I' + pack_fmt(r.fields)


# Check for an operation that has a file id type. Redact any user data
# if the redact flag is set, but print operations for file id 0, known
# to be the metadata.
def check_redact(optype):
    for f in optype.fields:
        if f.typename == 'uint32_id':
            redact_str = '\tif (!FLD_ISSET(args->flags, WT_TXN_PRINTLOG_UNREDACT) && '
            redact_str += '%s != WT_METAFILE_ID)\n' % (f.fieldname)
            redact_str += '\t\treturn(__wt_fprintf(session, args->fs, " REDACTED"));\n'
            return redact_str
    return ''


# Create a printf line, with an optional setup function.
# ishex indicates that the the field name in the output is modified
# (to add "-hex"), and that the setup and printf are conditional
# in the generated code.
def printf_line(f, optype, i, ishex):
    ifbegin = ''
    ifend = ''
    nl_indent = '\n\t'
    name = f.fieldname
    postcomma = '' if i + 1 == len(optype.fields) else ',\\n'
    precomma = ''
    if ishex > 0:
        name += '-hex'
        if not f.always_hex:
            ifend = nl_indent + '}'
            nl_indent += '\t'
            ifbegin = \
                'if (FLD_ISSET(args->flags, WT_TXN_PRINTLOG_HEX)) {' + nl_indent
            if postcomma == '':
                precomma = ',\\n'
    body = '%s%s(__wt_fprintf(session, args->fs,' % (
        f.printf_setup(ishex, nl_indent),
        'WT_ERR' if has_escape(optype.fields) else 'WT_RET') + \
        '%s    "%s        \\"%s\\": %s%s",%s));' % (
        nl_indent, precomma, name, f.printf_fmt(ishex), postcomma,
        f.printf_arg)
    return ifbegin + body + ifend

#####################################################################
# Create log_auto.c with handlers for each record / operation type.
#####################################################################

def run():
    f='../src/log/log_auto.c'
    tfile = open(tmp_file, 'w')

    tfile.write('/* DO NOT EDIT: automatically built by dist/log.py. */\n')

    tfile.write('''
#include "wt_internal.h"

#define WT_SIZE_CHECK_PACK_PTR(p, end)    WT_RET_TEST(!(p) || !(end) || (p) >= (end), ENOMEM)
#define WT_SIZE_CHECK_UNPACK_PTR(p, end)  WT_RET_TEST(!(p) || !(end) || (p) >= (end), EINVAL)

/*
 * __pack_encode__WT_ITEM --
 *\tPack a WT_ITEM structure.
 */

static inline int
__pack_encode__uintAny(uint8_t **pp, uint8_t *end, uint64_t item)
{
    /* Check that there is at least one byte available: the low-level routines treat zero length as unchecked. */
    WT_SIZE_CHECK_PACK_PTR(*pp, end);
    return __wt_vpack_uint(pp, WT_PTRDIFF(end, *pp), item);
}

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
__pack_encode__string(uint8_t **pp, uint8_t *end, const char *item)
{
    size_t s;

    s = __wt_strnlen(item, WT_PTRDIFF(end, *pp) - 1);
    WT_SIZE_CHECK_PACK(*pp, s + 1);
    memcpy(*pp, item, s);
    *pp += s;
    **pp = '\\0';
    *pp += 1;
    return (0);
}

#define __pack_decode__uintAny(TYPE, pval)  do { \
        uint64_t v; \
        /* Check that there is at least one byte available: the low-level routines treat zero length as unchecked. */ \
        WT_SIZE_CHECK_UNPACK_PTR(*pp, end); \
        WT_RET(__wt_vunpack_uint(pp, WT_PTRDIFF(end, *pp), &v)); \
        *(pval) = (TYPE)v; \
    } while (0)

#define __pack_decode__WT_ITEM(val)  do { \
        __pack_decode__uintAny(size_t, &val->size); \
        WT_SIZE_CHECK_UNPACK(val->size, WT_PTRDIFF(end, *pp)); \
        val->data = *pp; \
        *pp += val->size; \
    } while (0)

#define __pack_decode__string(val)  do { \
        size_t s; \
        *val = (const char *)*pp; \
        s = strlen((const char *)*pp) + 1; \
        WT_SIZE_CHECK_UNPACK(s, WT_PTRDIFF(end, *pp)); \
        *pp += s; \
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
\tsize_t allocsize;

\tlog = S2C(session)->log;
\tallocsize = log == NULL ? WT_LOG_ALIGN : log->allocsize;
\tWT_RET(__wt_scr_alloc(session, WT_ALIGN(size + 1, allocsize), &logrec));
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
static inline size_t
__wt_struct_size_%(name)s(%(arg_decls_in_or_void)s)
{
    return %(size_body)s;
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

\tsize = __wt_struct_size_%(name)s(%(pack_args)s);
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

#ifdef HAVE_DIAGNOSTIC
\tconst uint8_t **pp_orig;
\tpp_orig = pp;
#endif

\tif ((ret = __wt_logop_read(session, pp, end, &optype, &size)) != 0 ||
\t\t\t(ret = __wt_struct_unpack_%(name)s(pp, end%(comma)s%(unpack_args)s)) != 0)
\t\tWT_RET_MSG(session, ret, "logop_%(name)s: unpack failure");

\tWT_ASSERT(session, optype == %(macro)s);
#ifdef HAVE_DIAGNOSTIC
\tWT_ASSERT(session, WT_PTRDIFF(end, pp_orig) >= size);
#endif

\t*pp += size;
\treturn (0);
}
''' % {
            'name' : optype.name,
            'macro' : optype.macro_name,
            'comma' : ',' if optype.fields else '',
            'arg_decls_in' : ', '.join(f.cindecl for f in optype.fields),
            'arg_decls_in_or_void' : ', '.join(f.cindecl for f in optype.fields) if optype.fields else 'void',
            'arg_decls_out' : ', '.join(f.coutdecl for f in optype.fields),
            'size_body' : ' + '.join(f.struct_size_body() for f in optype.fields) if optype.fields else '0',
            'pack_args' : ', '.join(f.pack_arg() for f in optype.fields),
            'pack_body' : ''.join(f.struct_pack_body() for f in optype.fields) if optype.fields else '\tWT_UNUSED(pp);\n\tWT_UNUSED(end);',
            'unpack_args' : ', '.join(f.unpack_arg() for f in optype.fields),
            'unpack_body' : ''.join(f.struct_unpack_body() for f in optype.fields) if optype.fields else '\tWT_UNUSED(pp);\n\tWT_UNUSED(end);',
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
{%(arg_ret)s%(local_decls)s

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
            'local_decls' : (('\n' + '\n'.join("\t" + f.clocaldef + ";"
                for f in optype.fields)) + escape_decl(optype.fields)
                if optype.fields else ''),
            'arg_fini' : ('\nerr:\t__wt_scr_free(session, &escaped);\n\treturn (ret);'
            if has_escape(optype.fields) else '\treturn (0);'),
            'arg_addrs' : ''.join(', &%s' % f.fieldname for f in optype.fields),
            'redact' : check_redact(optype),
            'print_args' : ('\t' + '\n\t'.join(printf_line(f, optype, i, s)
                for i,f in enumerate(optype.fields) for s in range(0, f.n_setup))
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


if __name__ == "__main__":
    run()

