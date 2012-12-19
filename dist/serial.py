# Output serialization functions.

import textwrap
from dist import compare_srcfile

class SerialArg:
	def __init__(self, typestr, name, sized=0):
		self.typestr = typestr
		self.name = name
		self.sized = sized

class Serial:
	def __init__(self, name, args):
		self.name = name
		self.args = args

msgtypes = [
Serial('col_append', [
		SerialArg('WT_PAGE *', 'page'),
		SerialArg('uint32_t', 'write_gen'),
		SerialArg('WT_INSERT_HEAD **', 'insheadp'),
		SerialArg('WT_INSERT ***', 'ins_stack'),
		SerialArg('WT_INSERT **', 'next_stack'),
		SerialArg('WT_INSERT_HEAD **', 'new_inslist', 1),
		SerialArg('WT_INSERT_HEAD *', 'new_inshead', 1),
		SerialArg('WT_INSERT *', 'new_ins', 1),
		SerialArg('u_int', 'skipdepth'),
	]),

Serial('insert', [
		SerialArg('WT_PAGE *', 'page'),
		SerialArg('uint32_t', 'write_gen'),
		SerialArg('WT_INSERT_HEAD **', 'inshead'),
		SerialArg('WT_INSERT ***', 'ins_stack'),
		SerialArg('WT_INSERT **', 'next_stack'),
		SerialArg('WT_INSERT_HEAD **', 'new_inslist', 1),
		SerialArg('WT_INSERT_HEAD *', 'new_inshead', 1),
		SerialArg('WT_INSERT *', 'new_ins', 1),
		SerialArg('u_int', 'skipdepth'),
	]),

Serial('sync_file', [
		SerialArg('int', 'syncop'),
	]),

Serial('update', [
		SerialArg('WT_PAGE *', 'page'),
		SerialArg('uint32_t', 'write_gen'),
		SerialArg('WT_UPDATE **', 'srch_upd'),
		SerialArg('WT_UPDATE *', 'old_upd'),
		SerialArg('WT_UPDATE **', 'new_upd', 1),
		SerialArg('WT_UPDATE *', 'upd', 1),
		SerialArg('WT_UPDATE **', 'upd_obsolete'),
	]),
]

# decl --
#	Return a declaration for the variable.
def decl(l):
	o = l.typestr
	if o[-1] != '*':
		o += ' '
	return o + l.name

# decl_p --
#	Return a declaration for a reference to the variable, which requires
# another level of indirection.
def decl_p(l):
	o = l.typestr
	if o[-1] != '*':
		o += ' '
	return o + '*' + l.name + 'p'

# output --
#	Create serialized function calls.
def output(entry, f):
	# structure declaration
	f.write('''
typedef struct {
''')
	for l in entry.args:
		f.write('\t' + decl(l) + ';\n')
		if l.sized:
			f.write('\tsize_t ' + l.name + '_size;\n')
			f.write('\tint ' + l.name + '_taken;\n')
	f.write('} __wt_' + entry.name + '_args;\n\n')

	# pack function
	f.write('static inline int\n__wt_' + entry.name + '_serial(\n')
	o = 'WT_SESSION_IMPL *session'
	for l in entry.args:
		if l.sized:
			o += ', ' + decl_p(l) + ', size_t ' + l.name + '_size'
		else:
			o += ', ' + decl(l)
	o += ')'
	f.write('\n'.join('\t' + l for l in textwrap.wrap(o, 70)))
	f.write('''
{
\t__wt_''' + entry.name + '''_args _args, *args = &_args;
\tWT_DECL_RET;

''')
	for l in entry.args:
		if l.sized:
			f.write('''\tif (''' + l.name + '''p == NULL)
\t\targs->''' + l.name + ''' = NULL;
\telse {
\t\targs->''' + l.name + ''' = *''' + l.name + '''p;
\t\t*''' + l.name + '''p = NULL;
\t\targs->''' + l.name + '''_size = ''' + l.name + '''_size;
\t}
\targs->''' + l.name + '''_taken = 0;

''')
		else:
			f.write('\targs->' + l.name + ' = ' + l.name + ';\n\n')
	f.write('\t__wt_spin_lock(session, &S2C(session)->serial_lock);\n')
	f.write('\tret = __wt_' + entry.name + '_serial_func(session, args);\n')
	f.write('\t__wt_spin_unlock(session, &S2C(session)->serial_lock);\n\n')
	for l in entry.args:
		if not l.sized:
			continue
		f.write('\tif (!args->' + l.name + '_taken)\n')
		f.write('\t\t__wt_free(session, args->' + l.name + ');\n')
	f.write('\treturn (ret);\n')
	f.write('}\n\n')

	# unpack function
	f.write('static inline void\n__wt_' + entry.name + '_unpack(\n')
	o = 'void *untyped_args'
	for l in entry.args:
		o += ', ' + decl_p(l)
	o +=')'
	f.write('\n'.join('    ' + l for l in textwrap.wrap(o, 70)))
	f.write('''
{
\t__wt_''' + entry.name + '''_args *args = (__wt_''' + entry.name + '''_args *)untyped_args;

''')
	for l in entry.args:
		f.write('\t*' + l.name + 'p = args->' + l.name + ';\n')
	f.write('}\n')

	# taken functions
	for l in entry.args:
		if l.sized:
			f.write('''
static inline void\n__wt_''' + entry.name + '_' + l.name + '''_taken(
    WT_SESSION_IMPL *session, void *untyped_args, WT_PAGE *page)
{
\t__wt_''' + entry.name + '''_args *args = (__wt_''' + entry.name + '''_args *)untyped_args;

\targs->''' + l.name + '''_taken = 1;

\tWT_ASSERT(session, args->''' + l.name + '''_size != 0);
\t__wt_cache_page_inmem_incr(session, page, args->''' + l.name + '''_size);
}
''')

#####################################################################
# Update serial_funcs.i.
#####################################################################
tmp_file = '__tmp'
tfile = open(tmp_file, 'w')
tfile.write('/* DO NOT EDIT: automatically built by dist/serial.py. */\n')

for entry in msgtypes:
	output(entry, tfile)

tfile.close()

compare_srcfile(tmp_file, '../src/include/serial_funcs.i')
